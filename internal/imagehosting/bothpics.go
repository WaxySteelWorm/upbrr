// Copyright (c) 2025-2026, Audionut and the autobrr contributors.
// SPDX-License-Identifier: GPL-2.0-or-later

package imagehosting

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/html"

	"github.com/autobrr/upbrr/internal/redaction"
)

const (
	bothPicsOrigin         = "https://both.pics"
	bothPicsAPIURL         = "https://both.pics/v1/collections"
	bothPicsUserAgent      = "upbrr (https://github.com/autobrr/upbrr)"
	bothPicsPollInterval   = 1500 * time.Millisecond
	bothPicsPollTimeout    = 10 * time.Minute
	bothPicsThumbnailGrace = time.Minute
	bothPicsMaxImages      = 500
	// Collection responses include metadata for every image, unlike ordinary upload responses.
	bothPicsMaxResponseBytes int64 = 1024 * 1024
)

var bothPicsCollectionID = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

var errBothPicsGuestSessionExpired = errors.New("bothpics anonymous session expired or unavailable")

type bothPicsUploader struct {
	apiKey     string
	client     *http.Client
	guestMu    sync.Mutex
	guest      *bothPicsGuestSession
	guestReady chan struct{}
}

type bothPicsGuestSession struct {
	client *http.Client
	csrf   string
}

type bothPicsError struct {
	message string
	cause   error
}

func (e *bothPicsError) Error() string { return e.message }
func (e *bothPicsError) Unwrap() error { return e.cause }

type bothPicsCollection struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	URL    string `json:"url"`
	Frames []struct {
		ID    string `json:"id"`
		Label string `json:"label"`
		State string `json:"state"`
	} `json:"frames"`
	Images []bothPicsImage `json:"images"`
}

type bothPicsImage struct {
	ID       string `json:"id"`
	FrameID  string `json:"frameId"`
	State    string `json:"state"`
	URL      string `json:"url"`
	ThumbURL string `json:"thumbUrl"`
	SHA256   string `json:"sha256"`
}

func (u *bothPicsUploader) Upload(ctx context.Context, imagePath string) (uploadResult, error) {
	results, err := u.UploadBatch(ctx, []string{imagePath})
	if err != nil {
		return uploadResult{}, err
	}
	return results[0], nil
}

func (u *bothPicsUploader) UploadBatch(ctx context.Context, imagePaths []string) ([]uploadResult, error) {
	return u.UploadBatchWithName(ctx, imagePaths, "upbrr screenshots")
}

func (u *bothPicsUploader) UploadBatchWithName(ctx context.Context, imagePaths []string, galleryName string) (results []uploadResult, err error) {
	defer func() {
		if err == nil {
			return
		}
		message := err.Error()
		if key := strings.TrimSpace(u.apiKey); key != "" {
			message = strings.ReplaceAll(message, key, "[REDACTED]")
		}
		err = &bothPicsError{message: redaction.RedactValue(message, nil), cause: err}
	}()
	if len(imagePaths) == 0 || len(imagePaths) > bothPicsMaxImages {
		return nil, fmt.Errorf("bothpics upload requires between 1 and %d images", bothPicsMaxImages)
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("bothpics upload canceled: %w", err)
	}
	title := []rune(strings.TrimSpace(galleryName))
	if len(title) == 0 {
		title = []rune("upbrr screenshots")
	}
	if len(title) > 120 {
		title = title[:120]
	}
	labels := make([]string, len(imagePaths))
	files := make(map[string]string, len(imagePaths))
	for index, imagePath := range imagePaths {
		labels[index] = strconv.Itoa(index + 1)
		// postMultipartWithFields sorts file fields, so pad indexes to preserve input order.
		files[fmt.Sprintf("file%06d", index+1)] = imagePath
	}
	if strings.TrimSpace(u.apiKey) == "" {
		return u.uploadGuest(ctx, imagePaths, string(title), labels)
	}
	frameLabels, err := json.Marshal(labels)
	if err != nil {
		return nil, fmt.Errorf("bothpics encode frame labels: %w", err)
	}
	fields := map[string]string{
		"kind":        "screenshots",
		"title":       string(title),
		"visibility":  "unlisted",
		"frameLabels": string(frameLabels),
	}
	// Never forward the bearer token through redirects, including to subdomains.
	client := *u.client
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	body, status, err := postMultipartWithFields(ctx, &client, bothPicsAPIURL+"/upload", fields, files, u.headers())
	if err != nil {
		return nil, err
	}
	if status != http.StatusAccepted {
		return nil, bothPicsHTTPError("upload", status)
	}
	var accepted struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(body, &accepted); err != nil {
		return nil, fmt.Errorf("bothpics invalid upload response: %w", err)
	}
	if !bothPicsCollectionID.MatchString(accepted.ID) {
		return nil, errors.New("bothpics invalid upload response: collection ID missing or invalid")
	}
	// The response's statusUrl is not an authority for authenticated requests.
	return u.waitForCollection(ctx, &client, bothPicsAPIURL+"/"+accepted.ID, len(imagePaths))
}

func (u *bothPicsUploader) headers() map[string]string {
	headers := map[string]string{
		"User-Agent": bothPicsUserAgent,
		"Accept":     "application/json",
	}
	if key := strings.TrimSpace(u.apiKey); key != "" {
		headers["Authorization"] = "Bearer " + key
	}
	return headers
}

func (u *bothPicsUploader) uploadGuest(ctx context.Context, imagePaths []string, title string, labels []string) (results []uploadResult, err error) {
	// Validate local inputs before creating a remote guest or collection.
	for _, imagePath := range imagePaths {
		info, err := os.Stat(imagePath)
		if err != nil {
			return nil, fmt.Errorf("bothpics inspect upload image: %w", err)
		}
		if !info.Mode().IsRegular() || !isAllowedImageExt(imagePath) {
			return nil, errors.New("bothpics upload requires PNG, JPEG or WebP image files")
		}
	}
	session, err := u.guestSession(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err == nil {
			return
		}
		if errors.Is(err, errBothPicsGuestSessionExpired) {
			u.guestMu.Lock()
			if u.guest == session {
				u.guest = nil
			}
			u.guestMu.Unlock()
		}
		// Status requests do not send CSRF, but transport diagnostics must not expose it either.
		err = &bothPicsError{message: strings.ReplaceAll(err.Error(), session.csrf, "[REDACTED]"), cause: err}
	}()
	payload, err := json.Marshal(struct {
		Kind        string   `json:"kind"`
		Title       string   `json:"title"`
		FrameLabels []string `json:"frameLabels"`
		Visibility  string   `json:"visibility"`
	}{
		Kind:        "screenshots",
		Title:       title,
		FrameLabels: labels,
		Visibility:  "public",
	})
	if err != nil {
		return nil, fmt.Errorf("bothpics encode anonymous collection: %w", err)
	}
	req, err := bothPicsGuestRequest(ctx, http.MethodPost, "/app-api/collections", session.csrf, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	body, err := bothPicsDo(session.client, req, http.StatusCreated)
	if err != nil {
		return nil, err
	}
	var collection bothPicsCollection
	if err := json.Unmarshal(body, &collection); err != nil {
		return nil, fmt.Errorf("bothpics invalid anonymous collection response: %w", err)
	}
	imageIDs, err := collection.uploadSlots(len(imagePaths))
	if err != nil {
		return nil, err
	}
	basePath := "/app-api/collections/" + collection.ID
	for index, imagePath := range imagePaths {
		if err := session.uploadImage(ctx, basePath+"/images/"+imageIDs[index], imagePath); err != nil {
			return nil, err
		}
	}
	req, err = bothPicsGuestRequest(ctx, http.MethodPost, basePath+"/finalize", session.csrf, nil)
	if err != nil {
		return nil, err
	}
	if _, err := bothPicsDo(session.client, req, http.StatusAccepted); err != nil {
		return nil, err
	}
	return u.waitForCollection(ctx, session.client, bothPicsOrigin+basePath, len(imagePaths))
}

func (u *bothPicsUploader) guestSession(ctx context.Context) (*bothPicsGuestSession, error) {
	// Reuse this in-memory session across batches to avoid consuming guest signup quotas.
	for {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("bothpics anonymous session canceled: %w", err)
		}
		u.guestMu.Lock()
		if u.guest != nil && u.guest.hasSessionCookie() {
			session := u.guest
			u.guestMu.Unlock()
			return session, nil
		}
		u.guest = nil
		if ready := u.guestReady; ready != nil {
			u.guestMu.Unlock()
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("bothpics waiting for anonymous session: %w", ctx.Err())
			case <-ready:
				continue
			}
		}
		ready := make(chan struct{})
		u.guestReady = ready
		u.guestMu.Unlock()
		session, err := u.newGuestSession(ctx)
		u.guestMu.Lock()
		if err == nil {
			u.guest = session
		}
		u.guestReady = nil
		close(ready)
		u.guestMu.Unlock()
		return session, err
	}
}

func (s *bothPicsGuestSession) hasSessionCookie() bool {
	for _, cookie := range s.client.Jar.Cookies(&url.URL{Scheme: "https", Host: "both.pics"}) {
		if (cookie.Name == "__Host-bp_session" || cookie.Name == "bp_session") && cookie.Value != "" {
			return true
		}
	}
	return false
}

func (u *bothPicsUploader) newGuestSession(ctx context.Context) (*bothPicsGuestSession, error) {
	client := *u.client
	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, fmt.Errorf("bothpics create anonymous cookie jar: %w", err)
	}
	client.Jar = jar
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	pageRequest, err := bothPicsGuestRequest(ctx, http.MethodGet, "/screenshots/new", "", nil)
	if err != nil {
		return nil, err
	}
	if _, err := bothPicsDo(&client, pageRequest, http.StatusOK); err != nil {
		return nil, err
	}
	// Guest signup explicitly accepts the host's content policy; settings describe this mode.
	form := url.Values{"back": {"/screenshots/new"}, "accept": {"yes"}}
	req, err := bothPicsGuestRequest(ctx, http.MethodPost, "/guest", "", strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if _, err := bothPicsDo(&client, req, http.StatusSeeOther); err != nil {
		return nil, err
	}
	pageRequest, err = bothPicsGuestRequest(ctx, http.MethodGet, "/screenshots/new", "", nil)
	if err != nil {
		return nil, err
	}
	page, err := bothPicsDo(&client, pageRequest, http.StatusOK)
	if err != nil {
		return nil, err
	}
	csrf, err := bothPicsCSRF(page)
	if err != nil {
		return nil, err
	}
	session := &bothPicsGuestSession{client: &client, csrf: csrf}
	if !session.hasSessionCookie() {
		return nil, errors.New("bothpics anonymous session unavailable: session cookie missing")
	}
	return session, nil
}

func bothPicsCSRF(page []byte) (string, error) {
	tokens := html.NewTokenizer(bytes.NewReader(page))
	for tokenType := tokens.Next(); tokenType != html.ErrorToken; tokenType = tokens.Next() {
		if tokenType != html.StartTagToken {
			continue
		}
		token := tokens.Token()
		if token.Data != "script" {
			continue
		}
		for _, attr := range token.Attr {
			if attr.Key == "id" && attr.Val == "csrf" && tokens.Next() == html.TextToken {
				var csrf string
				if json.Unmarshal(tokens.Text(), &csrf) == nil && strings.TrimSpace(csrf) != "" {
					return csrf, nil
				}
				return "", errors.New("bothpics anonymous session CSRF response invalid")
			}
		}
	}
	return "", errors.New("bothpics anonymous session unavailable: CSRF token missing")
}

func (c *bothPicsCollection) uploadSlots(count int) ([]string, error) {
	if !bothPicsCollectionID.MatchString(c.ID) || len(c.Frames) != count || len(c.Images) != count {
		return nil, errors.New("bothpics anonymous collection response is incomplete")
	}
	images := make(map[string]string, count)
	seenIDs := make(map[string]bool, count)
	for _, image := range c.Images {
		if image.FrameID == "" || images[image.FrameID] != "" || seenIDs[image.ID] || !bothPicsCollectionID.MatchString(image.ID) {
			return nil, errors.New("bothpics anonymous collection image slots invalid")
		}
		images[image.FrameID] = image.ID
		seenIDs[image.ID] = true
	}
	ids := make([]string, count)
	for index, frame := range c.Frames {
		id := images[frame.ID]
		if id == "" || frame.Label != strconv.Itoa(index+1) {
			return nil, errors.New("bothpics anonymous collection frame mapping invalid")
		}
		ids[index] = id
		delete(images, frame.ID)
	}
	return ids, nil
}

func (s *bothPicsGuestSession) uploadImage(ctx context.Context, target, imagePath string) error {
	file, err := os.Open(imagePath)
	if err != nil {
		return fmt.Errorf("bothpics open upload image: %w", err)
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("bothpics inspect upload image: %w", err)
	}
	req, err := bothPicsGuestRequest(ctx, http.MethodPut, target, s.csrf, file)
	if err != nil {
		return err
	}
	req.ContentLength = info.Size()
	contentType := "image/png"
	switch strings.ToLower(filepath.Ext(imagePath)) {
	case ".jpg", ".jpeg":
		contentType = "image/jpeg"
	case ".webp":
		contentType = "image/webp"
	}
	req.Header.Set("Content-Type", contentType)
	_, err = bothPicsDo(s.client, req, http.StatusOK)
	return err
}

func bothPicsGuestRequest(ctx context.Context, method, target, csrf string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, bothPicsOrigin+target, body)
	if err != nil {
		return nil, fmt.Errorf("bothpics create anonymous request: %w", err)
	}
	req.Header.Set("User-Agent", bothPicsUserAgent)
	req.Header.Set("Accept", "application/json")
	if method != http.MethodGet {
		req.Header.Set("Origin", bothPicsOrigin)
		req.Header.Set("Content-Type", "application/json")
		if csrf != "" {
			req.Header.Set("X-Csrf-Token", csrf)
		}
	}
	return req, nil
}

func bothPicsDo(client *http.Client, req *http.Request, expectedStatus int) ([]byte, error) {
	resp, err := client.Do(req)
	if err != nil {
		closeResponseBody(resp)
		return nil, bothPicsRequestError(req, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != expectedStatus {
		if strings.HasPrefix(req.URL.Path, "/v1/") {
			return nil, bothPicsHTTPError("status request", resp.StatusCode)
		}
		if resp.StatusCode == http.StatusUnauthorized {
			return nil, fmt.Errorf("%w (status 401): configure an API token for account uploads", errBothPicsGuestSessionExpired)
		}
		return nil, fmt.Errorf(
			"bothpics anonymous request failed with status %d: guest access may be closed, expired or limited; configure an API token for account uploads",
			resp.StatusCode,
		)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, bothPicsMaxResponseBytes+1))
	if err != nil {
		return nil, bothPicsRequestError(req, fmt.Errorf("bothpics read response: %w", err))
	}
	if int64(len(body)) > bothPicsMaxResponseBytes {
		return nil, errors.New("bothpics response exceeds size limit")
	}
	return body, nil
}

func bothPicsRequestError(req *http.Request, err error) error {
	message := err.Error()
	for _, value := range []string{req.Header.Get("X-Csrf-Token"), req.Header.Get("Authorization")} {
		if value != "" {
			message = strings.ReplaceAll(message, value, "[REDACTED]")
		}
	}
	for _, cookie := range req.Cookies() {
		if cookie.Value != "" {
			message = strings.ReplaceAll(message, cookie.Value, "[REDACTED]")
		}
	}
	return &bothPicsError{message: redaction.RedactValue(message, nil), cause: err}
}

func (u *bothPicsUploader) waitForCollection(ctx context.Context, client *http.Client, target string, count int) ([]uploadResult, error) {
	ctx, cancel := context.WithTimeout(ctx, bothPicsPollTimeout)
	defer cancel()
	var thumbnailDeadline time.Time
	for {
		collection, err := u.getCollection(ctx, client, target)
		if err != nil {
			return nil, err
		}
		switch collection.Status {
		case "published":
			processing := false
			for _, frame := range collection.Frames {
				if frame.State == "pending" || frame.State == "processing" {
					processing = true
					break
				}
			}
			if !processing {
				results, err := collection.results(count)
				if err != nil {
					return nil, err
				}
				missingThumbnail := false
				for _, image := range collection.Images {
					if strings.TrimSpace(image.ThumbURL) == "" {
						missingThumbnail = true
						break
					}
				}
				if !missingThumbnail {
					return results, nil
				}
				// both.pics publishes originals before its thumbnail jobs finish.
				// Match the official client's grace period before falling back to originals.
				if thumbnailDeadline.IsZero() {
					thumbnailDeadline = time.Now().Add(bothPicsThumbnailGrace)
				} else if !time.Now().Before(thumbnailDeadline) {
					return results, nil
				}
			}
		case "draft", "processing":
		case "failed", "deleted":
			return nil, fmt.Errorf("bothpics collection %s; check image validation and account quota", collection.Status)
		default:
			return nil, errors.New("bothpics invalid collection status")
		}
		timer := time.NewTimer(bothPicsPollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, fmt.Errorf("bothpics waiting for publication: %w", ctx.Err())
		case <-timer.C:
		}
	}
}

func (u *bothPicsUploader) getCollection(ctx context.Context, client *http.Client, target string) (bothPicsCollection, error) {
	var collection bothPicsCollection
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return collection, fmt.Errorf("bothpics create status request: %w", err)
	}
	for name, value := range u.headers() {
		req.Header.Set(name, value)
	}
	body, err := bothPicsDo(client, req, http.StatusOK)
	if err != nil {
		return collection, err
	}
	if err := json.Unmarshal(body, &collection); err != nil {
		return collection, fmt.Errorf("bothpics invalid collection response: %w", err)
	}
	return collection, nil
}

func (c *bothPicsCollection) results(count int) ([]uploadResult, error) {
	if len(c.Frames) != count || len(c.Images) != count {
		return nil, errors.New("bothpics published collection image count mismatch")
	}
	pageURL := strings.TrimRight(strings.TrimSpace(c.URL), "/")
	if !bothPicsValidURL(pageURL) {
		return nil, errors.New("bothpics published collection URL missing or invalid")
	}
	images := make(map[string]bothPicsImage, count)
	for _, image := range c.Images {
		if _, duplicate := images[image.FrameID]; duplicate || image.FrameID == "" || image.State != "valid" {
			return nil, errors.New("bothpics published collection contains invalid images")
		}
		images[image.FrameID] = image
	}
	results := make([]uploadResult, count)
	for index, frame := range c.Frames {
		image, found := images[frame.ID]
		if !found || frame.State != "live" || frame.Label != strconv.Itoa(index+1) {
			return nil, errors.New("bothpics published collection frame mismatch")
		}
		delete(images, frame.ID)
		rawURL := strings.TrimSpace(image.URL)
		if !bothPicsValidURL(rawURL) {
			return nil, errors.New("bothpics published image URL missing or invalid")
		}
		if len(image.SHA256) >= 8 {
			parsed, _ := url.Parse(rawURL)
			query := parsed.Query()
			query.Set("v", image.SHA256[:8])
			parsed.RawQuery = query.Encode()
			rawURL = parsed.String()
		}
		thumbnail := strings.TrimSpace(image.ThumbURL)
		if thumbnail == "" {
			thumbnail = rawURL
		} else if !bothPicsValidURL(thumbnail) {
			return nil, errors.New("bothpics published thumbnail URL invalid")
		}
		results[index] = uploadResult{
			ImgURL: thumbnail,
			RawURL: rawURL,
			WebURL: pageURL + "/" + strconv.Itoa(index+1),
		}
	}
	return results, nil
}

func bothPicsValidURL(value string) bool {
	parsed, err := url.Parse(value)
	return err == nil && parsed.Scheme == "https" && parsed.Hostname() != "" && parsed.User == nil
}

func bothPicsHTTPError(operation string, status int) error {
	// Do not include server error bodies: they can echo credentials or private metadata.
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return fmt.Errorf("bothpics %s failed with status %d: check API token, read/upload scopes and account access", operation, status)
	case http.StatusTooManyRequests:
		return fmt.Errorf("bothpics %s failed with status %d: rate or daily quota limit reached", operation, status)
	default:
		return fmt.Errorf("bothpics %s failed with status %d", operation, status)
	}
}
