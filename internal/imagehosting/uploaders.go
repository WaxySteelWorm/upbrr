// Copyright (c) 2025-2026, Audionut and the autobrr contributors.
// SPDX-License-Identifier: GPL-2.0-or-later

package imagehosting

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/html"

	"github.com/autobrr/upbrr/internal/config"
	"github.com/autobrr/upbrr/internal/httpclient"
	"github.com/autobrr/upbrr/internal/redaction"
	"github.com/autobrr/upbrr/internal/trackers"
)

type uploadResult struct {
	ImgURL string
	RawURL string
	WebURL string
}

type uploader interface {
	Upload(ctx context.Context, imagePath string) (uploadResult, error)
}

type batchUploader interface {
	UploadBatch(ctx context.Context, imagePaths []string) ([]uploadResult, error)
}

type namedBatchUploader interface {
	UploadBatchWithName(ctx context.Context, imagePaths []string, galleryName string) ([]uploadResult, error)
}

const maxResponseBodyPreviewBytes int64 = 64 * 1024

func newUploaderRegistry(cfg config.Config, client *http.Client, registry *trackers.Registry) map[string]uploader {
	client = httpclient.CloneWithTimeout(client, httpclient.UploadTimeout)
	hdbConfig := ownedHostTrackerConfig(cfg, registry, "hdb")
	return map[string]uploader{
		"bothpics": &bothPicsUploader{apiKey: cfg.ImageHosting.BothPicsAPI, client: client},
		"imgbb":    &imgbbUploader{apiKey: cfg.ImageHosting.ImgBBAPI, client: client},
		"imgbox":   &imgboxUploader{client: client},
		"hdb": &hdbUploader{
			username: hdbConfig.Username,
			passkey:  hdbConfig.Passkey,
			client:   client,
		},
		"pixhost":   &pixhostUploader{client: client},
		"lensdump":  &lensdumpUploader{apiKey: cfg.ImageHosting.LensdumpAPI, client: client},
		"lostimg":   &lostimgUploader{apiKey: cfg.ImageHosting.LostimgAPI, client: client},
		"ptscreens": &ptScreensUploader{apiKey: cfg.ImageHosting.PTScreensAPI, client: client},
		"onlyimage": &onlyImageUploader{apiKey: cfg.ImageHosting.OnlyImageAPI, client: client},
		"dalexni":   &dalexniUploader{apiKey: cfg.ImageHosting.DalexniAPI, client: client},
		"zipline": &ziplineUploader{
			apiKey: cfg.ImageHosting.ZiplineAPIKey,
			url:    cfg.ImageHosting.ZiplineURL,
			client: client,
		},
		"passtheimage": &passTheImageUploader{apiKey: cfg.ImageHosting.PassTheImageAPI, client: client},
		"reelflix":     &reelflixUploader{apiKey: cfg.ImageHosting.ReelflixAPI, client: client},
		"samaritano":   &samaritanoUploader{apiKey: cfg.ImageHosting.SamaritanoAPI, client: client},
		"seedpool_cdn": &seedpoolUploader{apiKey: cfg.ImageHosting.SeedpoolCDNAPI, client: client},
		"sharex": &shareXUploader{
			apiKey: cfg.ImageHosting.ShareXAPIKey,
			url:    cfg.ImageHosting.ShareXURL,
			client: client,
		},
		"thr":   &thrUploader{apiKey: ownedHostTrackerConfig(cfg, registry, "thr").ImgAPI, client: client},
		"utppm": &utppmUploader{apiKey: cfg.ImageHosting.UTPPMAPI, client: client},
	}
}

// ownedHostTrackerConfig resolves private image-host credentials through the
// tracker manifest instead of coupling uploaders to tracker identifiers.
func ownedHostTrackerConfig(cfg config.Config, registry *trackers.Registry, host string) config.TrackerConfig {
	if registry == nil {
		return config.TrackerConfig{}
	}
	owner := registry.OwnerForImageHost(host)
	trackerConfig, _ := config.TrackerConfigByName(cfg.Trackers.Trackers, owner)
	return trackerConfig
}

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

type imgbbUploader struct {
	apiKey string
	client *http.Client
}

func (u *imgbbUploader) Upload(ctx context.Context, imagePath string) (uploadResult, error) {
	if strings.TrimSpace(u.apiKey) == "" {
		return uploadResult{}, errors.New("image hosting: imgbb api key missing")
	}
	encoded, err := readBase64(imagePath)
	if err != nil {
		return uploadResult{}, err
	}

	form := url.Values{}
	form.Set("key", strings.TrimSpace(u.apiKey))
	form.Set("image", encoded)

	body, status, err := postForm(ctx, u.client, "https://api.imgbb.com/1/upload", form, nil)
	if err != nil {
		return uploadResult{}, err
	}
	if status != http.StatusOK {
		return uploadResult{}, fmt.Errorf("imgbb upload failed with status %d", status)
	}

	var response struct {
		Success bool `json:"success"`
		Data    struct {
			Image struct {
				URL string `json:"url"`
			} `json:"image"`
			Thumb struct {
				URL string `json:"url"`
			} `json:"thumb"`
			Medium struct {
				URL string `json:"url"`
			} `json:"medium"`
			URLViewer string `json:"url_viewer"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return uploadResult{}, fmt.Errorf("imgbb invalid response: %w", err)
	}
	if !response.Success {
		return uploadResult{}, errors.New("imgbb upload failed")
	}
	imgURL := response.Data.Medium.URL
	if strings.TrimSpace(imgURL) == "" {
		imgURL = response.Data.Thumb.URL
	}

	return uploadResult{
		ImgURL: imgURL,
		RawURL: response.Data.Image.URL,
		WebURL: response.Data.URLViewer,
	}, nil
}

type imgboxUploader struct {
	client *http.Client
}

func (u *imgboxUploader) Upload(ctx context.Context, imagePath string) (uploadResult, error) {
	results, err := u.UploadBatch(ctx, []string{imagePath})
	if err != nil {
		return uploadResult{}, err
	}
	if len(results) != 1 {
		return uploadResult{}, errors.New("imgbox returned an invalid upload result count")
	}
	return results[0], nil
}

func (u *imgboxUploader) UploadBatch(ctx context.Context, imagePaths []string) ([]uploadResult, error) {
	if len(imagePaths) == 0 {
		return nil, errors.New("imgbox upload requires at least one image")
	}
	csrfToken, cookie, err := imgboxGetCsrfAndCookie(ctx, u.client)
	if err != nil {
		return nil, err
	}
	uploadToken, err := imgboxGetUploadToken(ctx, u.client, csrfToken, cookie)
	if err != nil {
		return nil, err
	}
	results := make([]uploadResult, len(imagePaths))
	errorsByIndex := make([]error, len(imagePaths))
	var wg sync.WaitGroup
	for index, imagePath := range imagePaths {
		wg.Go(func() {
			results[index], errorsByIndex[index] = u.uploadWithSession(ctx, imagePath, csrfToken, cookie, uploadToken)
		})
	}
	wg.Wait()
	if err := errors.Join(errorsByIndex...); err != nil {
		return nil, err
	}
	return results, nil
}

func (u *imgboxUploader) uploadWithSession(
	ctx context.Context,
	imagePath string,
	csrfToken string,
	cookie string,
	uploadToken imgboxUploadToken,
) (uploadResult, error) {
	fields := map[string]string{
		"token_id":         uploadToken.TokenID,
		"token_secret":     uploadToken.TokenSecret,
		"gallery_id":       uploadToken.GalleryID,
		"gallery_secret":   uploadToken.GallerySecret,
		"content_type":     "1",
		"thumbnail_size":   "350r",
		"comments_enabled": "0",
	}
	headers := imgboxUploadHeaders(cookie, csrfToken)
	body, status, err := postMultipart(ctx, u.client, "https://imgbox.com/upload/process", fields, "files[]", imagePath, headers)
	if err != nil {
		return uploadResult{}, fmt.Errorf("imgbox HTTP request failed: %w", err)
	}
	if status != http.StatusOK {
		return uploadResult{}, fmt.Errorf("imgbox upload unavailable (HTTP %d)", status)
	}

	var response struct {
		OK    bool `json:"ok"`
		Files []struct {
			OriginalURL  string `json:"original_url"`
			ThumbnailURL string `json:"thumbnail_url"`
			ImageURL     string `json:"image_url"`
			GalleryURL   string `json:"gallery_url"`
		} `json:"files"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return uploadResult{}, fmt.Errorf("imgbox upload returned invalid JSON: %w", err)
	}
	if !response.OK && len(response.Files) == 0 {
		errMsg := "unknown error"
		if message := safeResponseMessage(response.Error); message != "" {
			errMsg = message
		}
		return uploadResult{}, fmt.Errorf("imgbox upload rejected: %s", errMsg)
	}
	if len(response.Files) == 0 {
		return uploadResult{}, errors.New("imgbox returned no files in response")
	}

	file := response.Files[0]
	if file.OriginalURL == "" || file.ThumbnailURL == "" {
		return uploadResult{}, errors.New("imgbox returned incomplete upload URLs")
	}
	webURL := file.ImageURL
	if webURL == "" {
		webURL = file.GalleryURL
	}

	return uploadResult{
		ImgURL: file.ThumbnailURL,
		RawURL: file.OriginalURL,
		WebURL: webURL,
	}, nil
}

type imgboxUploadToken struct {
	TokenID       string
	TokenSecret   string
	GalleryID     string
	GallerySecret string
}

type hdbUploader struct {
	username string
	passkey  string
	client   *http.Client
}

var hdbUploadResultPattern = regexp.MustCompile(`\[url=([^\]]+)\]\[img\]([^\[]+)\[/img\]\[/url\]`)

const hdbMaxBatchUploadImages = 9

func (u *hdbUploader) Upload(ctx context.Context, imagePath string) (uploadResult, error) {
	results, err := u.uploadBatchWithGalleryName(ctx, []string{imagePath}, filepath.Base(imagePath))
	if err != nil {
		return uploadResult{}, err
	}
	return results[0], nil
}

func (u *hdbUploader) UploadBatch(ctx context.Context, imagePaths []string) ([]uploadResult, error) {
	if len(imagePaths) == 0 {
		return nil, errors.New("image hosting: no HDB images to upload")
	}
	galleryName := buildHDBGalleryName(imagePaths)
	return u.UploadBatchWithName(ctx, imagePaths, galleryName)
}

func (u *hdbUploader) UploadBatchWithName(ctx context.Context, imagePaths []string, galleryName string) ([]uploadResult, error) {
	if len(imagePaths) == 0 {
		return nil, errors.New("image hosting: no HDB images to upload")
	}
	galleryName = strings.TrimSpace(galleryName)
	if galleryName == "" {
		galleryName = buildHDBGalleryName(imagePaths)
	}
	if len(imagePaths) <= hdbMaxBatchUploadImages {
		return u.uploadBatchWithGalleryName(ctx, imagePaths, galleryName)
	}
	results := make([]uploadResult, 0, len(imagePaths))
	for start := 0; start < len(imagePaths); start += hdbMaxBatchUploadImages {
		end := min(start+hdbMaxBatchUploadImages, len(imagePaths))
		chunk, err := u.uploadBatchWithGalleryName(ctx, imagePaths[start:end], galleryName)
		if err != nil {
			return nil, err
		}
		results = append(results, chunk...)
	}
	return results, nil
}

func (u *hdbUploader) uploadBatchWithGalleryName(ctx context.Context, imagePaths []string, galleryName string) ([]uploadResult, error) {
	if strings.TrimSpace(u.username) == "" || strings.TrimSpace(u.passkey) == "" {
		return nil, errors.New("image hosting: hdb username/passkey missing")
	}
	fileFields := make(map[string]string, len(imagePaths))
	for idx, imagePath := range imagePaths {
		fileFields[fmt.Sprintf("images_files[%d]", idx)] = imagePath
	}
	body, status, err := postMultipartWithFields(ctx, u.client, "https://img.hdbits.org/upload_api.php", map[string]string{
		"username":      strings.TrimSpace(u.username),
		"passkey":       strings.TrimSpace(u.passkey),
		"galleryoption": "1",
		"galleryname":   galleryName,
		"thumbsize":     "w300",
	}, fileFields, nil)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("hdb upload failed with status %d", status)
	}
	results, err := parseHDBUploadResults(body)
	if err != nil {
		return nil, err
	}
	if len(results) != len(imagePaths) {
		return nil, fmt.Errorf("hdb upload returned %d images for %d uploads", len(results), len(imagePaths))
	}
	return results, nil
}

func buildHDBGalleryName(imagePaths []string) string {
	first := filepath.Base(strings.TrimSpace(imagePaths[0]))
	if first == "" {
		return "upbrr"
	}
	ext := filepath.Ext(first)
	base := strings.TrimSpace(strings.TrimSuffix(first, ext))
	if base == "" {
		base = "upbrr"
	}
	return base
}

func parseHDBUploadResults(body []byte) ([]uploadResult, error) {
	matches := hdbUploadResultPattern.FindAllStringSubmatch(string(body), -1)
	if len(matches) == 0 {
		return nil, errors.New("hdb upload did not return image bbcode")
	}
	results := make([]uploadResult, 0, len(matches))
	for _, match := range matches {
		if len(match) != 3 {
			return nil, errors.New("hdb upload returned malformed image bbcode")
		}
		rawURL := strings.TrimSpace(match[2])
		rawURL = strings.Replace(rawURL, "://t.hdbits.org/", "://img.hdbits.org/", 1)
		results = append(results, uploadResult{
			ImgURL: match[2],
			RawURL: rawURL,
			WebURL: match[1],
		})
	}
	return results, nil
}

func imgboxGetCsrfAndCookie(ctx context.Context, client *http.Client) (string, string, error) {
	client = httpclient.CloneWithTimeout(client, httpclient.UploadTimeout)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://imgbox.com/", nil)
	if err != nil {
		return "", "", fmt.Errorf("imgbox create csrf request: %w", err)
	}
	for key, value := range imgboxNavigationHeaders() {
		req.Header.Set(key, value)
	}
	resp, err := client.Do(req)
	if err != nil {
		closeResponseBody(resp)
		return "", "", fmt.Errorf("imgbox send csrf request: %w", err)
	}
	body, err := readLimitedAndCloseResponseBody(resp)
	if err != nil {
		return "", "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("imgbox anonymous upload session unavailable (HTTP %d)", resp.StatusCode)
	}
	contentType := strings.ToLower(strings.TrimSpace(resp.Header.Get("Content-Type")))
	if contentType != "" && !strings.HasPrefix(contentType, "text/html") {
		return "", "", errors.New("imgbox anonymous upload session returned unexpected content")
	}
	if imgboxChallengeDocument(string(body)) {
		return "", "", errors.New("imgbox anonymous upload session is temporarily challenged")
	}
	csrfToken, err := imgboxExtractCsrfToken(string(body))
	if err != nil {
		return "", "", err
	}
	cookie := imgboxPickCookie(resp)
	if cookie == "" {
		return "", "", errors.New("imgbox csrf cookie missing")
	}
	return csrfToken, cookie, nil
}

func imgboxGetUploadToken(ctx context.Context, client *http.Client, csrfToken string, cookie string) (imgboxUploadToken, error) {
	if strings.TrimSpace(csrfToken) == "" {
		return imgboxUploadToken{}, errors.New("imgbox csrf token missing")
	}
	if strings.TrimSpace(cookie) == "" {
		return imgboxUploadToken{}, errors.New("imgbox cookie missing")
	}
	client = httpclient.CloneWithTimeout(client, httpclient.UploadTimeout)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://imgbox.com/ajax/token/generate", nil)
	if err != nil {
		return imgboxUploadToken{}, fmt.Errorf("imgbox create token request: %w", err)
	}
	headers := imgboxUploadHeaders(cookie, csrfToken)
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	resp, err := client.Do(req)
	if err != nil {
		closeResponseBody(resp)
		return imgboxUploadToken{}, fmt.Errorf("imgbox send token request: %w", err)
	}
	body, err := readLimitedAndCloseResponseBody(resp)
	if err != nil {
		return imgboxUploadToken{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return imgboxUploadToken{}, fmt.Errorf("imgbox anonymous upload token unavailable (HTTP %d)", resp.StatusCode)
	}
	var tokenResp map[string]any
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return imgboxUploadToken{}, fmt.Errorf("imgbox token response invalid JSON: %w", err)
	}
	token := imgboxUploadToken{
		TokenID:       imgboxJSONValue(tokenResp["token_id"]),
		TokenSecret:   imgboxJSONValue(tokenResp["token_secret"]),
		GalleryID:     imgboxJSONValue(tokenResp["gallery_id"]),
		GallerySecret: imgboxJSONValue(tokenResp["gallery_secret"]),
	}
	if token.TokenID == "" || token.TokenID == "null" || token.TokenSecret == "" || token.TokenSecret == "null" {
		return imgboxUploadToken{}, errors.New("imgbox anonymous upload token response was incomplete")
	}
	return token, nil
}

func imgboxNavigationHeaders() map[string]string {
	return map[string]string{
		"Accept":          "text/html,application/xhtml+xml",
		"Accept-Language": "en-US,en;q=0.5",
		"User-Agent":      "upbrr",
	}
}

func imgboxUploadHeaders(cookie string, csrfToken string) map[string]string {
	return map[string]string{
		"Origin":           "https://imgbox.com",
		"Referer":          "https://imgbox.com/",
		"Accept":           "application/json, text/javascript, */*; q=0.01",
		"User-Agent":       "upbrr",
		"X-Requested-With": "XMLHttpRequest",
		"X-CSRF-Token":     csrfToken,
		"Cookie":           cookie,
	}
}

func imgboxChallengeDocument(body string) bool {
	normalized := strings.ToLower(body)
	return strings.Contains(normalized, "cf-chl-") ||
		strings.Contains(normalized, "attention required") ||
		strings.Contains(normalized, "checking your browser")
}

func imgboxExtractCsrfToken(body string) (string, error) {
	patterns := []string{
		`name="authenticity_token"[^>]*value="([^"]+)"`,
		`name='authenticity_token'[^>]*value='([^']+)'`,
	}
	for _, pattern := range patterns {
		re := regexp.MustCompile(pattern)
		match := re.FindStringSubmatch(body)
		if len(match) >= 2 && strings.TrimSpace(match[1]) != "" {
			return strings.TrimSpace(match[1]), nil
		}
	}
	return "", errors.New("imgbox authenticity token not found")
}

func imgboxPickCookie(resp *http.Response) string {
	if resp == nil {
		return ""
	}
	cookies := resp.Header.Values("Set-Cookie")
	if len(cookies) == 0 {
		return ""
	}
	parts := make([]string, 0, len(cookies))
	for _, raw := range cookies {
		segment, _, _ := strings.Cut(raw, ";")
		if strings.TrimSpace(segment) != "" {
			parts = append(parts, segment)
		}
	}
	return strings.Join(parts, "; ")
}

func imgboxJSONValue(value any) string {
	switch typed := value.(type) {
	case nil:
		return "null"
	case string:
		if strings.TrimSpace(typed) == "" {
			return "null"
		}
		return typed
	case json.Number:
		return typed.String()
	case float64:
		return strconv.FormatInt(int64(typed), 10)
	case int64:
		return strconv.FormatInt(typed, 10)
	case int:
		return strconv.Itoa(typed)
	default:
		return fmt.Sprintf("%v", typed)
	}
}

type dalexniUploader struct {
	apiKey string
	client *http.Client
}

func (u *dalexniUploader) Upload(ctx context.Context, imagePath string) (uploadResult, error) {
	if strings.TrimSpace(u.apiKey) == "" {
		return uploadResult{}, errors.New("image hosting: dalexni api key missing")
	}
	encoded, err := readBase64(imagePath)
	if err != nil {
		return uploadResult{}, err
	}
	form := url.Values{}
	form.Set("key", strings.TrimSpace(u.apiKey))
	form.Set("image", encoded)

	body, status, err := postForm(ctx, u.client, "https://dalexni.com/1/upload", form, nil)
	if err != nil {
		return uploadResult{}, err
	}
	if status != http.StatusOK {
		return uploadResult{}, fmt.Errorf("dalexni upload failed with status %d", status)
	}

	var response struct {
		Success bool `json:"success"`
		Data    struct {
			Image struct {
				URL string `json:"url"`
			} `json:"image"`
			Thumb struct {
				URL string `json:"url"`
			} `json:"thumb"`
			Medium struct {
				URL string `json:"url"`
			} `json:"medium"`
			URLViewer string `json:"url_viewer"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return uploadResult{}, fmt.Errorf("dalexni invalid response: %w", err)
	}
	if !response.Success {
		return uploadResult{}, errors.New("dalexni upload failed")
	}
	imgURL := response.Data.Medium.URL
	if strings.TrimSpace(imgURL) == "" {
		imgURL = response.Data.Thumb.URL
	}

	return uploadResult{
		ImgURL: imgURL,
		RawURL: response.Data.Image.URL,
		WebURL: response.Data.URLViewer,
	}, nil
}

type onlyImageUploader struct {
	apiKey string
	client *http.Client
}

func (u *onlyImageUploader) Upload(ctx context.Context, imagePath string) (uploadResult, error) {
	if strings.TrimSpace(u.apiKey) == "" {
		return uploadResult{}, errors.New("image hosting: onlyimage api key missing")
	}
	headers := map[string]string{"X-API-Key": strings.TrimSpace(u.apiKey)}

	body, status, err := postMultipart(ctx, u.client, "https://onlyimage.org/api/1/upload", nil, "source", imagePath, headers)
	if err != nil {
		return uploadResult{}, err
	}

	var response struct {
		StatusCode int `json:"status_code"`
		Success    struct {
			Code int `json:"code"`
		} `json:"success"`
		Image struct {
			Image struct {
				URL string `json:"url"`
			} `json:"image"`
			Medium struct {
				URL string `json:"url"`
			} `json:"medium"`
			Thumb struct {
				URL string `json:"url"`
			} `json:"thumb"`
			URL       string `json:"url"`
			URLViewer string `json:"url_viewer"`
		} `json:"image"`
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
		StatusText string `json:"status_txt"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		if status != http.StatusOK {
			return uploadResult{}, fmt.Errorf("onlyimage upload failed with status %d", status)
		}
		return uploadResult{}, fmt.Errorf("onlyimage invalid response: %w", err)
	}
	message := safeResponseMessage(response.Error.Message)
	if message == "" {
		message = safeResponseMessage(response.StatusText)
	}
	if status != http.StatusOK {
		if message == "" {
			return uploadResult{}, fmt.Errorf("onlyimage upload failed with status %d", status)
		}
		return uploadResult{}, fmt.Errorf("onlyimage upload failed: %s", message)
	}
	if response.StatusCode != http.StatusOK || (response.Success.Code != 0 && response.Success.Code != http.StatusOK) {
		if message == "" {
			message = "onlyimage upload failed"
		}
		return uploadResult{}, fmt.Errorf("onlyimage upload failed: %s", message)
	}
	rawURL := strings.TrimSpace(response.Image.URL)
	if rawURL == "" {
		rawURL = strings.TrimSpace(response.Image.Image.URL)
	}
	if rawURL == "" {
		return uploadResult{}, errors.New("onlyimage upload failed")
	}
	imgURL := strings.TrimSpace(response.Image.Medium.URL)
	if imgURL == "" {
		imgURL = strings.TrimSpace(response.Image.Thumb.URL)
	}
	if imgURL == "" {
		imgURL = rawURL
	}

	return uploadResult{
		ImgURL: imgURL,
		RawURL: rawURL,
		WebURL: response.Image.URLViewer,
	}, nil
}

type lensdumpUploader struct {
	apiKey string
	client *http.Client
}

func (u *lensdumpUploader) Upload(ctx context.Context, imagePath string) (uploadResult, error) {
	if strings.TrimSpace(u.apiKey) == "" {
		return uploadResult{}, errors.New("image hosting: lensdump api key missing")
	}
	encoded, err := readBase64(imagePath)
	if err != nil {
		return uploadResult{}, err
	}

	form := url.Values{}
	form.Set("image", encoded)
	headers := map[string]string{"X-API-Key": strings.TrimSpace(u.apiKey)}

	body, status, err := postForm(ctx, u.client, "https://lensdump.com/api/1/upload", form, headers)
	if err != nil {
		return uploadResult{}, err
	}
	if status != http.StatusOK {
		return uploadResult{}, fmt.Errorf("lensdump upload failed with status %d", status)
	}

	var response struct {
		StatusCode int `json:"status_code"`
		Data       struct {
			Image struct {
				URL string `json:"url"`
			} `json:"image"`
			URLViewer string `json:"url_viewer"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return uploadResult{}, fmt.Errorf("lensdump invalid response: %w", err)
	}
	if response.StatusCode != http.StatusOK {
		return uploadResult{}, errors.New("lensdump upload failed")
	}

	return uploadResult{
		ImgURL: response.Data.Image.URL,
		RawURL: response.Data.Image.URL,
		WebURL: response.Data.URLViewer,
	}, nil
}

type ptScreensUploader struct {
	apiKey string
	client *http.Client
}

func (u *ptScreensUploader) Upload(ctx context.Context, imagePath string) (uploadResult, error) {
	if strings.TrimSpace(u.apiKey) == "" {
		return uploadResult{}, errors.New("image hosting: ptscreens api key missing")
	}
	headers := map[string]string{"X-API-Key": strings.TrimSpace(u.apiKey)}
	body, status, err := postMultipart(ctx, u.client, "https://ptscreens.com/api/1/upload", nil, "source", imagePath, headers)
	if err != nil {
		return uploadResult{}, err
	}
	if status != http.StatusOK {
		return uploadResult{}, fmt.Errorf("ptscreens upload failed with status %d", status)
	}

	var response struct {
		Image struct {
			Medium struct {
				URL string `json:"url"`
			} `json:"medium"`
			URL       string `json:"url"`
			URLViewer string `json:"url_viewer"`
		} `json:"image"`
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return uploadResult{}, fmt.Errorf("ptscreens invalid response: %w", err)
	}
	if response.Image.URL == "" {
		return uploadResult{}, fmt.Errorf("ptscreens upload failed: %s", safeResponseMessage(response.Error.Message))
	}

	return uploadResult{
		ImgURL: response.Image.Medium.URL,
		RawURL: response.Image.URL,
		WebURL: response.Image.URLViewer,
	}, nil
}

type utppmUploader struct {
	apiKey string
	client *http.Client
}

func (u *utppmUploader) Upload(ctx context.Context, imagePath string) (uploadResult, error) {
	if strings.TrimSpace(u.apiKey) == "" {
		return uploadResult{}, errors.New("image hosting: utppm api key missing")
	}
	encoded, err := readBase64(imagePath)
	if err != nil {
		return uploadResult{}, err
	}

	form := url.Values{}
	form.Set("source", encoded)
	headers := map[string]string{"X-API-Key": strings.TrimSpace(u.apiKey)}

	body, status, err := postForm(ctx, u.client, "https://utp.pm/api/1/upload", form, headers)
	if err != nil {
		return uploadResult{}, err
	}
	if status != http.StatusOK {
		return uploadResult{}, fmt.Errorf("utppm upload failed with status %d", status)
	}

	var response struct {
		Image struct {
			Medium struct {
				URL string `json:"url"`
			} `json:"medium"`
			URL       string `json:"url"`
			URLViewer string `json:"url_viewer"`
		} `json:"image"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return uploadResult{}, fmt.Errorf("utppm invalid response: %w", err)
	}

	return uploadResult{
		ImgURL: response.Image.Medium.URL,
		RawURL: response.Image.URL,
		WebURL: response.Image.URLViewer,
	}, nil
}

type thrUploader struct {
	apiKey string
	client *http.Client
}

func (u *thrUploader) Upload(ctx context.Context, imagePath string) (uploadResult, error) {
	if strings.TrimSpace(u.apiKey) == "" {
		return uploadResult{}, errors.New("image hosting: thr api key missing")
	}
	body, status, err := postMultipart(ctx, u.client, "https://img2.torrenthr.org/api/1/upload", map[string]string{
		"key": strings.TrimSpace(u.apiKey),
	}, "source", imagePath, nil)
	if err != nil {
		return uploadResult{}, err
	}
	if status != http.StatusOK {
		return uploadResult{}, fmt.Errorf("thr upload failed with status %d", status)
	}

	var response struct {
		Image struct {
			URL string `json:"url"`
		} `json:"image"`
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return uploadResult{}, fmt.Errorf("thr invalid response: %w", err)
	}
	imageURL := strings.TrimSpace(response.Image.URL)
	if imageURL == "" {
		message := safeResponseMessage(response.Error.Message)
		if message == "" {
			message = "thr upload failed"
		}
		return uploadResult{}, fmt.Errorf("thr upload failed: %s", message)
	}
	return uploadResult{
		ImgURL: imageURL,
		RawURL: imageURL,
		WebURL: imageURL,
	}, nil
}

type lostimgUploader struct {
	apiKey string
	client *http.Client
}

const lostimgMaxBatchUploadImages = 50

func (u *lostimgUploader) Upload(ctx context.Context, imagePath string) (uploadResult, error) {
	results, err := u.UploadBatch(ctx, []string{imagePath})
	if err != nil {
		return uploadResult{}, err
	}
	return results[0], nil
}

func (u *lostimgUploader) UploadBatch(ctx context.Context, imagePaths []string) ([]uploadResult, error) {
	if strings.TrimSpace(u.apiKey) == "" {
		return nil, errors.New("image hosting: lostimg api key missing")
	}
	if len(imagePaths) == 0 {
		return nil, errors.New("image hosting: no Lostimg images to upload")
	}
	results := make([]uploadResult, 0, len(imagePaths))
	for start := 0; start < len(imagePaths); start += lostimgMaxBatchUploadImages {
		end := min(start+lostimgMaxBatchUploadImages, len(imagePaths))
		chunkResults, err := u.uploadBatch(ctx, imagePaths[start:end])
		results = append(results, chunkResults...)
		if err != nil {
			return results, err
		}
	}
	return results, nil
}

func (u *lostimgUploader) uploadBatch(ctx context.Context, imagePaths []string) ([]uploadResult, error) {
	headers := map[string]string{"Authorization": "Bearer " + strings.TrimSpace(u.apiKey)}
	body, status, err := postMultipartRepeatedFileField(ctx, u.client, "https://lostimg.cc/api/v1/images", "file[]", imagePaths, headers)
	if err != nil {
		return nil, err
	}
	var response struct {
		URL   string   `json:"url"`
		URLs  []string `json:"urls"`
		Error string   `json:"error"`
	}
	decodeErr := json.Unmarshal(body, &response)
	urls := response.URLs
	if len(urls) == 0 && strings.TrimSpace(response.URL) != "" {
		urls = []string{response.URL}
	}
	results := make([]uploadResult, 0, len(urls))
	emptyURL := false
	for _, raw := range urls {
		imageURL := strings.TrimSpace(raw)
		if imageURL == "" {
			emptyURL = true
			continue
		}
		results = append(results, uploadResult{
			ImgURL: imageURL,
			RawURL: imageURL,
			WebURL: imageURL,
		})
	}
	if status != http.StatusOK {
		return results, fmt.Errorf("lostimg upload failed with status %d", status)
	}
	if decodeErr != nil {
		return results, fmt.Errorf("lostimg invalid response: %w", decodeErr)
	}
	if safeResponseMessage(response.Error) != "" {
		return results, fmt.Errorf("lostimg upload failed: %s", safeResponseMessage(response.Error))
	}
	if len(urls) != len(imagePaths) {
		return results, fmt.Errorf("lostimg upload returned %d images for %d uploads", len(urls), len(imagePaths))
	}
	if emptyURL {
		return results, errors.New("lostimg upload returned empty image URL")
	}
	return results, nil
}

type pixhostUploader struct {
	client *http.Client
}

const pixhostUploadURL = "https://api.pixhost.to/images"

func (u *pixhostUploader) Upload(ctx context.Context, imagePath string) (uploadResult, error) {
	fields := map[string]string{
		"content_type": "0",
		"max_th_size":  "350",
	}
	body, status, err := postMultipart(ctx, u.client, pixhostUploadURL, fields, "img", imagePath, nil)
	if err != nil {
		return uploadResult{}, err
	}
	if status != http.StatusOK {
		return uploadResult{}, fmt.Errorf("pixhost upload failed with status %d", status)
	}

	var response struct {
		ThumbnailURL string `json:"th_url"`
		ShowURL      string `json:"show_url"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return uploadResult{}, fmt.Errorf("pixhost invalid response: %w", err)
	}
	if response.ThumbnailURL == "" {
		return uploadResult{}, errors.New("pixhost upload failed")
	}
	rawURL := strings.ReplaceAll(response.ThumbnailURL, "https://t", "https://img")
	rawURL = strings.ReplaceAll(rawURL, "/thumbs/", "/images/")

	return uploadResult{
		ImgURL: response.ThumbnailURL,
		RawURL: rawURL,
		WebURL: response.ShowURL,
	}, nil
}

type ziplineUploader struct {
	apiKey string
	url    string
	client *http.Client
}

func (u *ziplineUploader) Upload(ctx context.Context, imagePath string) (uploadResult, error) {
	if strings.TrimSpace(u.url) == "" || strings.TrimSpace(u.apiKey) == "" {
		return uploadResult{}, errors.New("image hosting: zipline url or api key missing")
	}
	headers := map[string]string{"Authorization": strings.TrimSpace(u.apiKey)}
	body, status, err := postMultipart(ctx, u.client, strings.TrimSpace(u.url), nil, "file", imagePath, headers)
	if err != nil {
		return uploadResult{}, err
	}
	if status != http.StatusOK {
		return uploadResult{}, fmt.Errorf("zipline upload failed with status %d", status)
	}

	var response struct {
		Files []string `json:"files"`
		Data  struct {
			Files []string `json:"files"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return uploadResult{}, fmt.Errorf("zipline invalid response: %w", err)
	}
	files := response.Files
	if len(files) == 0 {
		files = response.Data.Files
	}
	if len(files) == 0 {
		return uploadResult{}, errors.New("zipline upload failed")
	}
	urlValue := strings.TrimSpace(files[0])
	if urlValue == "" {
		return uploadResult{}, errors.New("zipline upload failed")
	}
	rawURL := strings.Replace(urlValue, "/u/", "/r/", 1)

	return uploadResult{
		ImgURL: urlValue,
		RawURL: rawURL,
		WebURL: rawURL,
	}, nil
}

type passTheImageUploader struct {
	apiKey string
	client *http.Client
}

func (u *passTheImageUploader) Upload(ctx context.Context, imagePath string) (uploadResult, error) {
	if strings.TrimSpace(u.apiKey) == "" {
		return uploadResult{}, errors.New("image hosting: passtheimage api key missing")
	}
	headers := map[string]string{"X-API-Key": strings.TrimSpace(u.apiKey)}
	body, status, err := postMultipart(ctx, u.client, "https://passtheima.ge/api/1/upload", nil, "source", imagePath, headers)
	if err != nil {
		return uploadResult{}, err
	}
	if status != http.StatusOK {
		return uploadResult{}, fmt.Errorf("passtheimage upload failed with status %d", status)
	}

	var response struct {
		StatusCode int `json:"status_code"`
		Image      struct {
			URL       string `json:"url"`
			URLViewer string `json:"url_viewer"`
		} `json:"image"`
		Error struct {
			Message string `json:"message"`
			Code    string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return uploadResult{}, fmt.Errorf("passtheimage invalid response: %w", err)
	}
	if response.StatusCode != http.StatusOK {
		message := safeResponseMessage(response.Error.Message)
		if message == "" {
			message = "passtheimage upload failed"
		}
		return uploadResult{}, fmt.Errorf("passtheimage upload failed: %s", message)
	}
	if response.Image.URL == "" {
		return uploadResult{}, errors.New("passtheimage upload failed")
	}

	return uploadResult{
		ImgURL: response.Image.URL,
		RawURL: response.Image.URL,
		WebURL: response.Image.URLViewer,
	}, nil
}

type reelflixUploader struct {
	apiKey string
	client *http.Client
}

func (u *reelflixUploader) Upload(ctx context.Context, imagePath string) (uploadResult, error) {
	if strings.TrimSpace(u.apiKey) == "" {
		return uploadResult{}, errors.New("image hosting: reelflix api key missing")
	}
	headers := map[string]string{"X-API-Key": strings.TrimSpace(u.apiKey)}
	body, status, err := postMultipart(ctx, u.client, "https://img.reelflix.cc/api/1/upload", nil, "source", imagePath, headers)
	if err != nil {
		return uploadResult{}, err
	}
	if status != http.StatusOK {
		return uploadResult{}, fmt.Errorf("reelflix upload failed with status %d", status)
	}

	var response struct {
		StatusCode int `json:"status_code"`
		Image      struct {
			Medium struct {
				URL string `json:"url"`
			} `json:"medium"`
			URL       string `json:"url"`
			URLViewer string `json:"url_viewer"`
		} `json:"image"`
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return uploadResult{}, fmt.Errorf("reelflix invalid response: %w", err)
	}
	if response.StatusCode != 0 && response.StatusCode != http.StatusOK {
		message := safeResponseMessage(response.Error.Message)
		if message == "" {
			message = "reelflix upload failed"
		}
		return uploadResult{}, fmt.Errorf("reelflix upload failed: %s", message)
	}
	if response.Image.URL == "" {
		return uploadResult{}, errors.New("reelflix upload failed")
	}
	imgURL := response.Image.Medium.URL
	if strings.TrimSpace(imgURL) == "" {
		imgURL = response.Image.URL
	}

	return uploadResult{
		ImgURL: imgURL,
		RawURL: response.Image.URL,
		WebURL: response.Image.URLViewer,
	}, nil
}

type seedpoolUploader struct {
	apiKey string
	client *http.Client
}

func (u *seedpoolUploader) Upload(ctx context.Context, imagePath string) (uploadResult, error) {
	if strings.TrimSpace(u.apiKey) == "" {
		return uploadResult{}, errors.New("image hosting: seedpool api key missing")
	}
	headers := map[string]string{"Authorization": "Bearer " + strings.TrimSpace(u.apiKey)}
	body, status, err := postMultipart(ctx, u.client, "https://i.seedpool.org/upload", nil, "files[]", imagePath, headers)
	if err != nil {
		return uploadResult{}, err
	}
	if status != http.StatusOK && status != http.StatusCreated {
		return uploadResult{}, fmt.Errorf("seedpool upload failed with status %d", status)
	}

	var response struct {
		Files []struct {
			URL          string            `json:"url"`
			Variants     map[string]string `json:"variants"`
			ThumbnailURL string            `json:"thumbnail_url"`
		} `json:"files"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return uploadResult{}, fmt.Errorf("seedpool invalid response: %w", err)
	}
	if len(response.Files) == 0 {
		return uploadResult{}, errors.New("seedpool upload failed")
	}
	file := response.Files[0]
	imgURL := strings.TrimSpace(file.ThumbnailURL)
	if imgURL == "" {
		if variant, ok := file.Variants["thumb"]; ok {
			imgURL = variant
		}
	}
	if imgURL == "" {
		if variant, ok := file.Variants["medium"]; ok {
			imgURL = variant
		}
	}
	if imgURL == "" {
		imgURL = file.URL
	}

	return uploadResult{
		ImgURL: imgURL,
		RawURL: file.URL,
		WebURL: file.URL,
	}, nil
}

type shareXUploader struct {
	apiKey string
	url    string
	client *http.Client
}

const samaritanoUploadURL = "https://img.samaritano.cc/api/v1/sharex/upload"

type samaritanoUploader struct {
	apiKey string
	client *http.Client
}

func (u *samaritanoUploader) Upload(ctx context.Context, imagePath string) (uploadResult, error) {
	if strings.TrimSpace(u.apiKey) == "" {
		return uploadResult{}, errors.New("image hosting: samaritano api key missing")
	}
	body, status, err := postMultipart(ctx, u.client, samaritanoUploadURL, nil, "file", imagePath, map[string]string{
		"Authorization": "Bearer " + strings.TrimSpace(u.apiKey),
	})
	if err != nil {
		return uploadResult{}, err
	}
	if status != http.StatusOK && status != http.StatusCreated {
		return uploadResult{}, fmt.Errorf("samaritano upload failed with status %d", status)
	}

	var response struct {
		URL          string `json:"url"`
		ThumbnailURL string `json:"thumbnail_url"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return uploadResult{}, fmt.Errorf("samaritano invalid response: %w", err)
	}
	urlValue := strings.TrimSpace(response.URL)
	if urlValue == "" {
		return uploadResult{}, errors.New("samaritano upload failed: response URL missing")
	}
	imgURL := strings.TrimSpace(response.ThumbnailURL)
	if imgURL == "" {
		imgURL = urlValue
	}
	return uploadResult{
		ImgURL: imgURL,
		RawURL: urlValue,
		WebURL: urlValue,
	}, nil
}

func (u *shareXUploader) Upload(ctx context.Context, imagePath string) (uploadResult, error) {
	urlValue := strings.TrimSpace(u.url)
	if urlValue == "" {
		urlValue = "https://img.digitalcore.club/api/upload"
	}
	if strings.TrimSpace(u.apiKey) == "" {
		return uploadResult{}, errors.New("image hosting: sharex api key missing")
	}
	headers := map[string]string{"Authorization": strings.TrimSpace(u.apiKey)}
	fields := map[string]string{"title": "upbrr screenshot"}
	body, status, err := postMultipart(ctx, u.client, urlValue, fields, "file", imagePath, headers)
	if err != nil {
		return uploadResult{}, err
	}
	if status != http.StatusOK && status != http.StatusCreated {
		return uploadResult{}, fmt.Errorf("sharex upload failed with status %d", status)
	}

	var response struct {
		Data struct {
			Link string `json:"link"`
		} `json:"data"`
		Link    string `json:"link"`
		Message string `json:"message"`
		Error   string `json:"error"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return uploadResult{}, fmt.Errorf("sharex invalid response: %w", err)
	}
	link := strings.TrimSpace(response.Data.Link)
	if link == "" {
		link = strings.TrimSpace(response.Link)
	}
	if link == "" {
		message := safeResponseMessage(response.Message)
		if message == "" {
			message = safeResponseMessage(response.Error)
		}
		if message == "" {
			message = "sharex upload failed"
		}
		return uploadResult{}, fmt.Errorf("sharex upload failed: %s", message)
	}

	return uploadResult{
		ImgURL: link,
		RawURL: link,
		WebURL: link,
	}, nil
}

func postForm(ctx context.Context, client *http.Client, target string, data url.Values, headers map[string]string) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, 0, fmt.Errorf("image hosting: create form request for %s: %w", target, err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	resp, err := client.Do(req)
	if err != nil {
		closeResponseBody(resp)
		return nil, 0, fmt.Errorf("image hosting: send form request to %s: %w", target, err)
	}
	body, err := readLimitedAndCloseResponseBody(resp)
	if err != nil {
		return nil, resp.StatusCode, err
	}
	return body, resp.StatusCode, nil
}

func postMultipart(
	ctx context.Context,
	client *http.Client,
	target string,
	fields map[string]string,
	fileField string,
	filePath string,
	headers map[string]string,
) ([]byte, int, error) {
	return postMultipartWithFields(ctx, client, target, fields, map[string]string{fileField: filePath}, headers)
}

func postMultipartWithFields(
	ctx context.Context,
	client *http.Client,
	target string,
	fields map[string]string,
	fileFields map[string]string,
	headers map[string]string,
) ([]byte, int, error) {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	fieldKeys := make([]string, 0, len(fields))
	for key := range fields {
		fieldKeys = append(fieldKeys, key)
	}
	sort.Strings(fieldKeys)
	for _, key := range fieldKeys {
		value := fields[key]
		if err := writer.WriteField(key, value); err != nil {
			return nil, 0, fmt.Errorf("image hosting: write multipart field %q: %w", key, err)
		}
	}
	fileFieldKeys := make([]string, 0, len(fileFields))
	for key := range fileFields {
		fileFieldKeys = append(fileFieldKeys, key)
	}
	sort.Strings(fileFieldKeys)
	for _, fileField := range fileFieldKeys {
		filePath := fileFields[fileField]
		file, err := os.Open(filePath)
		if err != nil {
			return nil, 0, fmt.Errorf("image hosting: open multipart file: %w", err)
		}
		part, err := writer.CreateFormFile(fileField, filepath.Base(filePath))
		if err != nil {
			_ = file.Close()
			return nil, 0, fmt.Errorf("image hosting: create multipart file %q: %w", fileField, err)
		}
		if _, err := io.Copy(part, file); err != nil {
			_ = file.Close()
			return nil, 0, fmt.Errorf("image hosting: copy multipart file: %w", err)
		}
		if err := file.Close(); err != nil {
			return nil, 0, fmt.Errorf("image hosting: close multipart file: %w", err)
		}
	}
	if err := writer.Close(); err != nil {
		return nil, 0, fmt.Errorf("image hosting: close multipart writer: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, body)
	if err != nil {
		return nil, 0, fmt.Errorf("image hosting: create multipart request for %s: %w", target, err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	resp, err := client.Do(req)
	if err != nil {
		closeResponseBody(resp)
		return nil, 0, fmt.Errorf("image hosting: send multipart request to %s: %w", target, err)
	}
	bodyBytes, err := readLimitedAndCloseResponseBody(resp)
	if err != nil {
		return nil, resp.StatusCode, err
	}
	return bodyBytes, resp.StatusCode, nil
}

func postMultipartRepeatedFileField(
	ctx context.Context,
	client *http.Client,
	target string,
	fileField string,
	filePaths []string,
	headers map[string]string,
) ([]byte, int, error) {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	for _, filePath := range filePaths {
		file, err := os.Open(filePath)
		if err != nil {
			return nil, 0, fmt.Errorf("image hosting: open multipart file: %w", err)
		}
		part, err := writer.CreateFormFile(fileField, filepath.Base(filePath))
		if err != nil {
			_ = file.Close()
			return nil, 0, fmt.Errorf("image hosting: create multipart file %q: %w", fileField, err)
		}
		if _, err := io.Copy(part, file); err != nil {
			_ = file.Close()
			return nil, 0, fmt.Errorf("image hosting: copy multipart file: %w", err)
		}
		if err := file.Close(); err != nil {
			return nil, 0, fmt.Errorf("image hosting: close multipart file: %w", err)
		}
	}
	if err := writer.Close(); err != nil {
		return nil, 0, fmt.Errorf("image hosting: close multipart writer: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, body)
	if err != nil {
		return nil, 0, fmt.Errorf("image hosting: create multipart request for %s: %w", target, err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	resp, err := client.Do(req)
	if err != nil {
		closeResponseBody(resp)
		return nil, 0, fmt.Errorf("image hosting: send multipart request to %s: %w", target, err)
	}
	bodyBytes, err := readLimitedAndCloseResponseBody(resp)
	if err != nil {
		return nil, resp.StatusCode, err
	}
	return bodyBytes, resp.StatusCode, nil
}

// readLimitedAndCloseResponseBody reads only the diagnostic preview cap from a
// response body and always closes the body before returning.
func readLimitedAndCloseResponseBody(resp *http.Response) ([]byte, error) {
	if resp == nil || resp.Body == nil {
		return nil, nil
	}
	defer closeResponseBody(resp)
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodyPreviewBytes))
	if err != nil {
		return nil, fmt.Errorf("image hosting: read response body: %w", err)
	}
	return body, nil
}

// safeResponsePreview returns a redacted, length-bounded response snippet for
// upload errors and diagnostics.
func safeResponsePreview(body []byte) string {
	text := safeResponseMessage(string(body))
	if len(text) > 200 {
		text = strings.TrimSpace(text[:200]) + "..."
	}
	return text
}

// safeResponseMessage redacts response text before it reaches errors or logs.
func safeResponseMessage(value string) string {
	return strings.TrimSpace(redaction.RedactValue(value, nil))
}

func closeResponseBody(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}

func readBase64(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("image hosting: read base64 file: %w", err)
	}
	return base64.StdEncoding.EncodeToString(data), nil
}
