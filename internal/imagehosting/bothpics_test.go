// Copyright (c) 2025-2026, Audionut and the autobrr contributors.
// SPDX-License-Identifier: GPL-2.0-or-later

package imagehosting

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/autobrr/upbrr/internal/config"
	"github.com/autobrr/upbrr/internal/httpclient"
)

const bothPicsTestCollectionID = "c06c91e5-1d7f-4a39-a91f-c75a2e807260"

const bothPicsTestAccepted = `{"id":"` + bothPicsTestCollectionID + `","status":"processing","statusUrl":"https://untrusted.example.invalid/private"}`

type bothPicsTestFrame struct {
	ID    string `json:"id"`
	Label string `json:"label"`
	State string `json:"state"`
}

type bothPicsTestImage struct {
	ID       string `json:"id"`
	FrameID  string `json:"frameId"`
	State    string `json:"state"`
	URL      string `json:"url"`
	ThumbURL string `json:"thumbUrl"`
	SHA256   string `json:"sha256"`
}

type bothPicsTestCollection struct {
	ID      string              `json:"id"`
	Status  string              `json:"status"`
	URL     string              `json:"url"`
	Frames  []bothPicsTestFrame `json:"frames"`
	Images  []bothPicsTestImage `json:"images"`
	Padding string              `json:"padding,omitempty"`
}

func newBothPicsTestCollection(count int) bothPicsTestCollection {
	collection := bothPicsTestCollection{
		ID:     bothPicsTestCollectionID,
		Status: "published",
		URL:    "https://both.pics/s/synthetic",
		Frames: make([]bothPicsTestFrame, count),
		Images: make([]bothPicsTestImage, count),
	}
	for index := range count {
		frameID := fmt.Sprintf("frame-%d", index+1)
		collection.Frames[index] = bothPicsTestFrame{
			ID:    frameID,
			Label: strconv.Itoa(index + 1),
			State: "live",
		}
		collection.Images[index] = bothPicsTestImage{
			ID:       fmt.Sprintf("00000000-0000-4000-8000-%012d", index+1),
			FrameID:  frameID,
			State:    "valid",
			URL:      fmt.Sprintf("https://both.pics/m/synthetic/image-%d", index+1),
			ThumbURL: fmt.Sprintf("https://both.pics/t/synthetic/image-%d", index+1),
			SHA256:   strings.Repeat("abcdef12", 8),
		}
	}
	return collection
}

func bothPicsTestJSON(t *testing.T, value any) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal both.pics fixture: %v", err)
	}
	return string(encoded)
}

func bothPicsTestPaths(t *testing.T, count int) []string {
	t.Helper()
	root := t.TempDir()
	paths := make([]string, count)
	for index := range paths {
		paths[index] = filepath.Join(root, fmt.Sprintf("synthetic-%d.png", index+1))
		if err := os.WriteFile(paths[index], fmt.Appendf(nil, "synthetic image %d", index+1), 0o600); err != nil {
			t.Fatalf("write image fixture: %v", err)
		}
	}
	return paths
}

func TestBothPicsBatchRequestAndOrderedResults(t *testing.T) {
	t.Parallel()

	// Twelve parts also exercise ordering beyond a single-digit multipart field.
	paths := bothPicsTestPaths(t, 12)
	collection := newBothPicsTestCollection(len(paths))
	collection.Images[2].SHA256 = ""
	slices.Reverse(collection.Images)
	requests := 0
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests++
		if got := request.Header.Get("Authorization"); got != "Bearer synthetic-api-key" {
			t.Fatal("incorrect Authorization header")
		}
		if got := request.Header.Get("Accept"); got != "application/json" {
			t.Fatalf("Accept = %q", got)
		}
		if got := request.Header.Get("User-Agent"); got != "upbrr (https://github.com/autobrr/upbrr)" {
			t.Fatalf("User-Agent = %q", got)
		}
		switch requests {
		case 1:
			if request.Method != http.MethodPost || request.URL.String() != "https://both.pics/v1/collections/upload" {
				t.Fatalf("upload request = %s %s", request.Method, request.URL)
			}
			assertBothPicsMultipart(t, request, paths, "Synthetic.Release-GRP")
			return imageTestResponse(http.StatusAccepted, bothPicsTestAccepted), nil
		case 2:
			if request.Method != http.MethodGet || request.URL.String() != "https://both.pics/v1/collections/"+bothPicsTestCollectionID {
				t.Fatalf("poll request = %s %s", request.Method, request.URL)
			}
			return imageTestResponse(http.StatusOK, bothPicsTestJSON(t, collection)), nil
		default:
			t.Fatalf("unexpected request %d", requests)
			return nil, nil
		}
	})}

	results, err := (&bothPicsUploader{apiKey: "  synthetic-api-key\n", client: client}).UploadBatchWithName(
		t.Context(), paths, "  Synthetic.Release-GRP  ",
	)
	if err != nil {
		t.Fatalf("upload batch: %v", err)
	}
	if len(results) != len(paths) || requests != 2 {
		t.Fatalf("results = %d, requests = %d", len(results), requests)
	}
	for index, result := range results {
		rawURL := fmt.Sprintf("https://both.pics/m/synthetic/image-%d", index+1)
		if index != 2 {
			rawURL += "?v=abcdef12"
		}
		thumbURL := fmt.Sprintf("https://both.pics/t/synthetic/image-%d", index+1)
		want := uploadResult{
			ImgURL: thumbURL,
			RawURL: rawURL,
			WebURL: fmt.Sprintf("https://both.pics/s/synthetic/%d", index+1),
		}
		if result != want {
			t.Fatalf("result %d = %+v, want %+v", index, result, want)
		}
	}
}

func assertBothPicsMultipart(t *testing.T, request *http.Request, paths []string, title string) {
	t.Helper()
	mediaType, params, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "multipart/form-data" || params["boundary"] == "" {
		t.Fatalf("multipart Content-Type = %q: %v", request.Header.Get("Content-Type"), err)
	}
	reader := multipart.NewReader(request.Body, params["boundary"])
	fields := make(map[string]string)
	fileIndex := 0
	for {
		part, partErr := reader.NextPart()
		if errors.Is(partErr, io.EOF) {
			break
		}
		if partErr != nil {
			t.Fatalf("next multipart part: %v", partErr)
		}
		body, readErr := io.ReadAll(part)
		if closeErr := part.Close(); closeErr != nil {
			t.Fatalf("close multipart part: %v", closeErr)
		}
		if readErr != nil {
			t.Fatalf("read multipart part: %v", readErr)
		}
		if part.FileName() == "" {
			if fileIndex != 0 {
				t.Fatal("metadata field followed file parts")
			}
			fields[part.FormName()] = string(body)
			continue
		}
		if len(fields) != 4 {
			t.Fatalf("metadata fields before files = %v", fields)
		}
		if fileIndex >= len(paths) {
			t.Fatal("too many file parts")
		}
		if got, want := part.FormName(), fmt.Sprintf("file%06d", fileIndex+1); got != want {
			t.Fatalf("file field = %q, want %q", got, want)
		}
		if got, want := part.FileName(), filepath.Base(paths[fileIndex]); got != want {
			t.Fatalf("file name = %q, want %q", got, want)
		}
		if got, want := string(body), fmt.Sprintf("synthetic image %d", fileIndex+1); got != want {
			t.Fatalf("file contents = %q, want %q", got, want)
		}
		fileIndex++
	}
	labels := make([]string, len(paths))
	for index := range labels {
		labels[index] = strconv.Itoa(index + 1)
	}
	want := map[string]string{
		"kind":        "screenshots",
		"title":       title,
		"visibility":  "unlisted",
		"frameLabels": bothPicsTestJSON(t, labels),
	}
	if !reflect.DeepEqual(fields, want) || fileIndex != len(paths) {
		t.Fatalf("multipart fields = %v, files = %d; want %v, %d", fields, fileIndex, want, len(paths))
	}
}

func TestBothPicsSingleUploadAndLargeCollectionResponse(t *testing.T) {
	t.Parallel()
	paths := bothPicsTestPaths(t, 1)
	collection := newBothPicsTestCollection(1)
	collection.Padding = strings.Repeat("x", 70*1024)
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method == http.MethodPost {
			if err := request.ParseMultipartForm(1 << 20); err != nil {
				t.Fatalf("parse single upload: %v", err)
			}
			if request.FormValue("title") == "" {
				t.Fatal("default collection title is empty")
			}
			return imageTestResponse(http.StatusAccepted, bothPicsTestAccepted), nil
		}
		return imageTestResponse(http.StatusOK, bothPicsTestJSON(t, collection)), nil
	})}
	result, err := (&bothPicsUploader{apiKey: "synthetic-api-key", client: client}).Upload(t.Context(), paths[0])
	if err != nil {
		t.Fatalf("single upload with large collection response: %v", err)
	}
	if result.WebURL != "https://both.pics/s/synthetic/1" || result.RawURL != "https://both.pics/m/synthetic/image-1?v=abcdef12" {
		t.Fatalf("single result = %+v", result)
	}
}

func TestBothPicsRejectsInvalidInputsWithoutRequest(t *testing.T) {
	t.Parallel()
	paths := bothPicsTestPaths(t, 1)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	scenarios := []struct {
		name  string
		key   string
		paths []string
		ctx   context.Context
	}{
		{
			name:  "anonymous missing file",
			paths: []string{filepath.Join(t.TempDir(), "missing.png")},
			ctx:   t.Context(),
		},
		{
			name: "empty batch",
			key:  "synthetic-api-key",
			ctx:  t.Context(),
		},
		{
			name:  "missing file",
			key:   "synthetic-api-key",
			paths: []string{filepath.Join(t.TempDir(), "missing.png")},
			ctx:   t.Context(),
		},
		{
			name:  "too many images",
			key:   "synthetic-api-key",
			paths: slices.Repeat(paths, 501),
			ctx:   t.Context(),
		},
		{
			name:  "canceled context",
			key:   "synthetic-api-key",
			paths: paths,
			ctx:   ctx,
		},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				t.Fatal("invalid input caused an HTTP request")
				return nil, nil
			})}
			_, err := (&bothPicsUploader{apiKey: scenario.key, client: client}).UploadBatch(scenario.ctx, scenario.paths)
			if err == nil {
				t.Fatal("invalid input succeeded")
			}
			if scenario.name == "canceled context" && !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled upload error = %v", err)
			}
		})
	}
}

func TestBothPicsRejectsUploadAndPollingErrors(t *testing.T) {
	t.Parallel()
	paths := bothPicsTestPaths(t, 1)
	const privateMarker = "synthetic-private-response-marker"
	scenarios := []struct {
		name       string
		postStatus int
		postBody   string
		getStatus  int
		getBody    string
		wantCalls  int
	}{
		{
			name:       "unauthorized upload",
			postStatus: http.StatusUnauthorized,
			postBody:   privateMarker,
			wantCalls:  1,
		},
		{
			name:       "rate limited upload",
			postStatus: http.StatusTooManyRequests,
			postBody:   privateMarker,
			wantCalls:  1,
		},
		{
			name:       "unexpected upload status",
			postStatus: http.StatusOK,
			postBody:   bothPicsTestAccepted,
			wantCalls:  1,
		},
		{
			name:       "malformed upload JSON",
			postStatus: http.StatusAccepted,
			postBody:   `{"id":"` + privateMarker,
			wantCalls:  1,
		},
		{
			name:       "missing collection ID",
			postStatus: http.StatusAccepted,
			postBody:   `{"status":"processing"}`,
			wantCalls:  1,
		},
		{
			name:       "unsafe collection ID",
			postStatus: http.StatusAccepted,
			postBody:   `{"id":"../` + privateMarker + `"}`,
			wantCalls:  1,
		},
		{
			name:       "upload response too large",
			postStatus: http.StatusAccepted,
			postBody:   strings.Repeat("x", (1<<20)+1),
			wantCalls:  1,
		},
		{
			name:      "unauthorized polling",
			getStatus: http.StatusUnauthorized,
			getBody:   privateMarker,
			wantCalls: 2,
		},
		{
			name:      "missing collection",
			getStatus: http.StatusNotFound,
			getBody:   privateMarker,
			wantCalls: 2,
		},
		{
			name:      "poll server failure",
			getStatus: http.StatusInternalServerError,
			getBody:   privateMarker,
			wantCalls: 2,
		},
		{
			name:      "malformed polling JSON",
			getStatus: http.StatusOK,
			getBody:   `{"status":"` + privateMarker,
			wantCalls: 2,
		},
		{
			name:      "poll response too large",
			getStatus: http.StatusOK,
			getBody:   strings.Repeat("x", (1<<20)+1),
			wantCalls: 2,
		},
		{
			name:      "failed collection",
			getStatus: http.StatusOK,
			getBody:   `{"status":"failed","error":"` + privateMarker + `"}`,
			wantCalls: 2,
		},
		{
			name:      "deleted collection",
			getStatus: http.StatusOK,
			getBody:   `{"status":"deleted"}`,
			wantCalls: 2,
		},
		{
			name:      "unknown collection status",
			getStatus: http.StatusOK,
			getBody:   `{"status":"` + privateMarker + `"}`,
			wantCalls: 2,
		},
		{
			name:      "missing collection status",
			getStatus: http.StatusOK,
			getBody:   `{}`,
			wantCalls: 2,
		},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			calls := 0
			client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				calls++
				if calls > scenario.wantCalls {
					t.Fatal("unexpected retry after terminal error")
				}
				if request.Method == http.MethodPost {
					if scenario.postStatus == 0 {
						return imageTestResponse(http.StatusAccepted, bothPicsTestAccepted), nil
					}
					return imageTestResponse(scenario.postStatus, scenario.postBody), nil
				}
				return imageTestResponse(scenario.getStatus, scenario.getBody), nil
			})}
			_, err := (&bothPicsUploader{apiKey: "synthetic-api-key", client: client}).UploadBatch(t.Context(), paths)
			if err == nil {
				t.Fatal("invalid response succeeded")
			}
			if strings.Contains(err.Error(), privateMarker) {
				t.Fatalf("error exposed remote response: %v", err)
			}
			if calls != scenario.wantCalls {
				t.Fatalf("HTTP requests = %d, want %d", calls, scenario.wantCalls)
			}
		})
	}
}

func TestBothPicsRejectsInvalidPublishedImages(t *testing.T) {
	t.Parallel()
	paths := bothPicsTestPaths(t, 2)
	scenarios := []struct {
		name   string
		mutate func(*bothPicsTestCollection)
	}{
		{name: "missing collection URL", mutate: func(c *bothPicsTestCollection) { c.URL = "" }},
		{name: "relative collection URL", mutate: func(c *bothPicsTestCollection) { c.URL = "/s/synthetic" }},
		{name: "unsafe collection URL", mutate: func(c *bothPicsTestCollection) { c.URL = "javascript:alert(1)" }},
		{name: "missing frame", mutate: func(c *bothPicsTestCollection) { c.Frames = c.Frames[:1] }},
		{name: "missing image", mutate: func(c *bothPicsTestCollection) { c.Images = c.Images[:1] }},
		{name: "extra image", mutate: func(c *bothPicsTestCollection) { c.Images = append(c.Images, c.Images[0]) }},
		{name: "duplicate frame ID", mutate: func(c *bothPicsTestCollection) { c.Frames[1].ID = c.Frames[0].ID }},
		{name: "missing frame ID", mutate: func(c *bothPicsTestCollection) { c.Frames[0].ID = "" }},
		{name: "wrong frame label", mutate: func(c *bothPicsTestCollection) { c.Frames[1].Label = "1" }},
		{name: "reordered frames", mutate: func(c *bothPicsTestCollection) { slices.Reverse(c.Frames) }},
		{name: "duplicate image frame", mutate: func(c *bothPicsTestCollection) { c.Images[1].FrameID = c.Images[0].FrameID }},
		{name: "unknown image frame", mutate: func(c *bothPicsTestCollection) { c.Images[0].FrameID = "unknown-frame" }},
		{name: "invalid frame", mutate: func(c *bothPicsTestCollection) { c.Frames[0].State = "rejected" }},
		{name: "invalid image", mutate: func(c *bothPicsTestCollection) { c.Images[0].State = "invalid" }},
		{name: "missing raw URL", mutate: func(c *bothPicsTestCollection) { c.Images[0].URL = "" }},
		{name: "relative raw URL", mutate: func(c *bothPicsTestCollection) { c.Images[0].URL = "/m/synthetic/image-1" }},
		{name: "unsafe raw URL", mutate: func(c *bothPicsTestCollection) { c.Images[0].URL = "file:///synthetic.png" }},
		{name: "invalid thumbnail URL", mutate: func(c *bothPicsTestCollection) { c.Images[0].ThumbURL = "data:image/png;base64,AA==" }},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			collection := newBothPicsTestCollection(len(paths))
			scenario.mutate(&collection)
			polls := 0
			client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				if request.Method == http.MethodPost {
					return imageTestResponse(http.StatusAccepted, bothPicsTestAccepted), nil
				}
				polls++
				if polls > 1 {
					t.Fatal("invalid published collection was retried")
				}
				return imageTestResponse(http.StatusOK, bothPicsTestJSON(t, collection)), nil
			})}
			results, err := (&bothPicsUploader{apiKey: "synthetic-api-key", client: client}).UploadBatch(t.Context(), paths)
			if err == nil || len(results) != 0 {
				t.Fatalf("invalid collection results = %+v, error = %v", results, err)
			}
		})
	}
}

func TestBothPicsWaitsForPublishedAndReadyImages(t *testing.T) {
	paths := bothPicsTestPaths(t, 1)
	synctest.Test(t, func(t *testing.T) {
		polls := 0
		start := time.Now()
		client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			if request.Method == http.MethodPost {
				return imageTestResponse(http.StatusAccepted, bothPicsTestAccepted), nil
			}
			polls++
			if elapsed, want := time.Since(start), time.Duration(polls-1)*1500*time.Millisecond; elapsed != want {
				t.Fatalf("poll %d after %s, want %s", polls, elapsed, want)
			}
			collection := newBothPicsTestCollection(1)
			switch polls {
			case 1:
				return imageTestResponse(http.StatusOK, `{"status":"processing"}`), nil
			case 2:
				collection.Frames[0].State = "processing"
				collection.Images[0].State = "pending"
				collection.Images[0].URL = ""
			case 3:
				collection.Images[0].ThumbURL = ""
			case 4:
			default:
				t.Fatal("completed collection was polled again")
			}
			return imageTestResponse(http.StatusOK, bothPicsTestJSON(t, collection)), nil
		})}
		results, err := (&bothPicsUploader{apiKey: "synthetic-api-key", client: client}).UploadBatch(t.Context(), paths)
		if err != nil || len(results) != 1 || polls != 4 {
			t.Fatalf("results = %+v, error = %v, polls = %d", results, err, polls)
		}
		if results[0].ImgURL != "https://both.pics/t/synthetic/image-1" {
			t.Fatalf("thumbnail = %q", results[0].ImgURL)
		}
	})
}

func TestBothPicsFallsBackAfterThumbnailGracePeriod(t *testing.T) {
	paths := bothPicsTestPaths(t, 1)
	synctest.Test(t, func(t *testing.T) {
		collection := newBothPicsTestCollection(1)
		collection.Images[0].ThumbURL = ""
		polls := 0
		client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			if request.Method == http.MethodPost {
				return imageTestResponse(http.StatusAccepted, bothPicsTestAccepted), nil
			}
			polls++
			return imageTestResponse(http.StatusOK, bothPicsTestJSON(t, collection)), nil
		})}
		start := time.Now()
		results, err := (&bothPicsUploader{apiKey: "synthetic-api-key", client: client}).UploadBatch(t.Context(), paths)
		if err != nil || len(results) != 1 {
			t.Fatalf("thumbnail fallback results = %+v, error = %v", results, err)
		}
		if elapsed := time.Since(start); elapsed != time.Minute || polls != 41 {
			t.Fatalf("thumbnail grace period = %s, polls = %d", elapsed, polls)
		}
		if results[0].ImgURL != results[0].RawURL || results[0].RawURL == "" {
			t.Fatalf("thumbnail fallback result = %+v", results[0])
		}
	})
}

func TestBothPicsPollingHonorsCancellationAndDeadline(t *testing.T) {
	paths := bothPicsTestPaths(t, 1)
	for _, cancelAfter := range []time.Duration{time.Second, 10 * time.Minute} {
		t.Run(cancelAfter.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				wantErr := context.DeadlineExceeded
				if cancelAfter < 10*time.Minute {
					wantErr = context.Canceled
					time.AfterFunc(cancelAfter, cancel)
				}
				polls := 0
				client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
					if request.Method == http.MethodPost {
						return imageTestResponse(http.StatusAccepted, bothPicsTestAccepted), nil
					}
					polls++
					return imageTestResponse(http.StatusOK, `{"status":"processing"}`), nil
				})}
				start := time.Now()
				_, err := (&bothPicsUploader{apiKey: "synthetic-api-key", client: client}).UploadBatch(ctx, paths)
				if !errors.Is(err, wantErr) {
					t.Fatalf("polling error = %v, want %v", err, wantErr)
				}
				if elapsed := time.Since(start); elapsed != cancelAfter || polls == 0 {
					t.Fatalf("polling elapsed = %s, polls = %d", elapsed, polls)
				}
			})
		})
	}
}

func TestBothPicsTransportErrorsDoNotExposeSecrets(t *testing.T) {
	t.Parallel()
	paths := bothPicsTestPaths(t, 1)
	const syntheticAPIKey = "synthetic-api-key"
	for _, failMethod := range []string{http.MethodPost, http.MethodGet} {
		t.Run(failMethod, func(t *testing.T) {
			transportErr := errors.New("transport failure with " + syntheticAPIKey)
			client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				if request.Method == failMethod {
					return nil, transportErr
				}
				return imageTestResponse(http.StatusAccepted, bothPicsTestAccepted), nil
			})}
			_, err := (&bothPicsUploader{apiKey: syntheticAPIKey, client: client}).UploadBatch(t.Context(), paths)
			if err == nil {
				t.Fatal("transport failure succeeded")
			}
			if strings.Contains(err.Error(), syntheticAPIKey) {
				t.Fatal("transport error exposed the configured API key")
			}
			if !errors.Is(err, transportErr) {
				t.Fatal("redacted transport error did not preserve its cause")
			}
		})
	}
}

func TestBothPicsRejectsRedirectsWithoutForwardingToken(t *testing.T) {
	t.Parallel()
	paths := bothPicsTestPaths(t, 1)
	for _, redirectMethod := range []string{http.MethodPost, http.MethodGet} {
		t.Run(redirectMethod, func(t *testing.T) {
			redirected := false
			client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				if redirected {
					t.Fatalf("followed authenticated redirect to %s", request.URL)
				}
				if request.Method == redirectMethod {
					redirected = true
					response := imageTestResponse(http.StatusTemporaryRedirect, "")
					response.Header.Set("Location", "https://redirect.both.pics/private")
					return response, nil
				}
				return imageTestResponse(http.StatusAccepted, bothPicsTestAccepted), nil
			})}
			_, err := (&bothPicsUploader{apiKey: "synthetic-api-key", client: client}).UploadBatch(t.Context(), paths)
			if err == nil || !redirected {
				t.Fatalf("redirect error = %v, reached redirect = %t", err, redirected)
			}
			if client.CheckRedirect != nil {
				t.Fatal("uploader changed shared client redirect policy")
			}
		})
	}
}

func TestBothPicsRegistryUsesConfiguredKeyAndUploadTimeout(t *testing.T) {
	t.Parallel()
	cfg := config.Config{}
	cfg.ImageHosting.BothPicsAPI = "synthetic-api-key"
	baseClient := &http.Client{Timeout: 2 * time.Second}
	registered := newUploaderRegistry(cfg, baseClient, nil)
	u, ok := registered["bothpics"].(*bothPicsUploader)
	if !ok {
		t.Fatalf("registered bothpics uploader = %T", registered["bothpics"])
	}
	if u.apiKey != cfg.ImageHosting.BothPicsAPI {
		t.Fatal("registry did not use configured both.pics API key")
	}
	if u.client.Timeout != httpclient.UploadTimeout || baseClient.Timeout != 2*time.Second || u.client == baseClient {
		t.Fatalf("uploader timeout = %s, original timeout = %s", u.client.Timeout, baseClient.Timeout)
	}
}

func TestBothPicsRegistryAllowsAnonymousWithoutAPIKey(t *testing.T) {
	t.Parallel()
	registered := newUploaderRegistry(config.Config{}, &http.Client{}, nil)
	u, ok := registered["bothpics"].(*bothPicsUploader)
	if !ok || u.apiKey != "" {
		t.Fatal("registry did not retain the anonymous both.pics option without credentials")
	}
}

const (
	bothPicsTestGuestCookie = "synthetic-guest-session-secret"
	bothPicsTestDevice      = "synthetic-device-secret"
	bothPicsTestCSRF        = "synthetic-csrf-secret"
)

func bothPicsTestGuestClient(t *testing.T, count int, intercept roundTripFunc) *http.Client {
	t.Helper()
	return &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Scheme != "https" || request.URL.Host != "both.pics" {
			t.Fatal("anonymous request left fixed service origin")
		}
		if request.Header.Get("Authorization") != "" {
			t.Fatal("anonymous request included Authorization")
		}
		if intercept != nil {
			response, err := intercept(request)
			if response != nil || err != nil {
				return response, err
			}
		}
		if request.Method != http.MethodGet && request.Header.Get("Origin") != "https://both.pics" {
			t.Fatal("anonymous mutation omitted fixed Origin")
		}
		if strings.HasPrefix(request.URL.Path, "/app-api/") {
			cookie, err := request.Cookie("__Host-bp_session")
			if err != nil || cookie.Value != bothPicsTestGuestCookie {
				t.Fatal("anonymous request omitted session cookie")
			}
			if request.Method != http.MethodGet && request.Header.Get("X-Csrf-Token") != bothPicsTestCSRF {
				t.Fatal("anonymous mutation omitted CSRF token")
			}
		}
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/screenshots/new":
			if _, err := request.Cookie("__Host-bp_session"); err == nil {
				return imageTestResponse(http.StatusOK, `<html><script type="application/json" id="csrf">"`+bothPicsTestCSRF+`"</script></html>`), nil
			}
			response := imageTestResponse(http.StatusOK, "<html>Guest offer</html>")
			response.Header.Add("Set-Cookie", "__Host-bp_device="+bothPicsTestDevice+"; Path=/; Secure; HttpOnly")
			return response, nil
		case request.Method == http.MethodPost && request.URL.Path == "/guest":
			cookie, err := request.Cookie("__Host-bp_device")
			if err != nil || cookie.Value != bothPicsTestDevice {
				t.Fatal("guest signup omitted device cookie")
			}
			if err := request.ParseForm(); err != nil {
				t.Fatalf("parse guest form: %v", err)
			}
			if request.Form.Get("accept") != "yes" || request.Form.Get("back") != "/screenshots/new" {
				t.Fatal("guest signup form did not accept content policy or select screenshot uploader")
			}
			response := imageTestResponse(http.StatusSeeOther, "")
			response.Header.Add("Set-Cookie", "__Host-bp_session="+bothPicsTestGuestCookie+"; Path=/; Secure; HttpOnly")
			response.Header.Set("Location", "/screenshots/new")
			return response, nil
		case request.Method == http.MethodPost && request.URL.Path == "/app-api/collections":
			var payload struct {
				Kind       string   `json:"kind"`
				Title      string   `json:"title"`
				Visibility string   `json:"visibility"`
				Labels     []string `json:"frameLabels"`
			}
			if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
				t.Fatalf("decode guest create payload: %v", err)
			}
			if payload.Kind != "screenshots" || payload.Title == "" || payload.Visibility != "public" || len(payload.Labels) != count {
				t.Fatalf("anonymous collection payload = %+v", payload)
			}
			for index, label := range payload.Labels {
				if label != strconv.Itoa(index+1) {
					t.Fatal("anonymous frame labels do not preserve input order")
				}
			}
			collection := newBothPicsTestCollection(count)
			collection.Status = "draft"
			slices.Reverse(collection.Images)
			return imageTestResponse(http.StatusCreated, bothPicsTestJSON(t, collection)), nil
		case request.Method == http.MethodPut:
			for index := range count {
				id := fmt.Sprintf("00000000-0000-4000-8000-%012d", index+1)
				if request.URL.Path != "/app-api/collections/"+bothPicsTestCollectionID+"/images/"+id {
					continue
				}
				body, err := io.ReadAll(request.Body)
				if err != nil {
					t.Fatalf("read anonymous image upload: %v", err)
				}
				if string(body) != fmt.Sprintf("synthetic image %d", index+1) || request.ContentLength != int64(len(body)) {
					t.Fatal("anonymous upload body did not match its ordered image slot")
				}
				if request.Header.Get("Content-Type") != "image/png" {
					t.Fatalf("anonymous image Content-Type = %q", request.Header.Get("Content-Type"))
				}
				return imageTestResponse(http.StatusOK, `{"image":{"id":"`+id+`","state":"uploaded"}}`), nil
			}
		case request.Method == http.MethodPost && request.URL.Path == "/app-api/collections/"+bothPicsTestCollectionID+"/finalize":
			return imageTestResponse(http.StatusAccepted, `{"status":"processing"}`), nil
		case request.Method == http.MethodGet && request.URL.Path == "/app-api/collections/"+bothPicsTestCollectionID:
			return imageTestResponse(http.StatusOK, bothPicsTestJSON(t, newBothPicsTestCollection(count))), nil
		}
		t.Fatalf("unexpected anonymous request = %s %s", request.Method, request.URL.Path)
		return nil, nil
	})}
}

func TestBothPicsAnonymousUploadWithoutAPIKey(t *testing.T) {
	t.Parallel()
	paths := bothPicsTestPaths(t, 2)
	for _, key := range []string{"", " \t\n "} {
		t.Run(fmt.Sprintf("key_length_%d", len(key)), func(t *testing.T) {
			var signups atomic.Int32
			var uploads atomic.Int32
			client := bothPicsTestGuestClient(t, len(paths), func(request *http.Request) (*http.Response, error) {
				if request.URL.Path == "/guest" {
					signups.Add(1)
				}
				if request.Method == http.MethodPut {
					uploads.Add(1)
				}
				return nil, nil
			})
			uploader := &bothPicsUploader{apiKey: key, client: client}
			for range 2 {
				results, err := uploader.UploadBatchWithName(t.Context(), paths, "Synthetic.Release-GRP")
				if err != nil || len(results) != len(paths) {
					t.Fatalf("anonymous results = %+v, error = %v", results, err)
				}
				for index, result := range results {
					if result.WebURL != fmt.Sprintf("https://both.pics/s/synthetic/%d", index+1) {
						t.Fatalf("anonymous result %d = %+v", index, result)
					}
				}
			}
			if signups.Load() != 1 || uploads.Load() != 4 {
				t.Fatalf("guest signups = %d, image uploads = %d", signups.Load(), uploads.Load())
			}
			if client.Jar != nil || client.CheckRedirect != nil {
				t.Fatal("anonymous uploader changed shared client session settings")
			}
		})
	}
}

func TestBothPicsConcurrentAnonymousUploadsReuseSession(t *testing.T) {
	t.Parallel()
	paths := bothPicsTestPaths(t, 1)
	var signups atomic.Int32
	client := bothPicsTestGuestClient(t, 1, func(request *http.Request) (*http.Response, error) {
		if request.URL.Path == "/guest" {
			signups.Add(1)
		}
		return nil, nil
	})
	uploader := &bothPicsUploader{client: client}
	var group sync.WaitGroup
	for range 3 {
		group.Go(func() {
			if _, err := uploader.UploadBatch(t.Context(), paths); err != nil {
				t.Errorf("concurrent anonymous upload: %v", err)
			}
		})
	}
	group.Wait()
	if signups.Load() != 1 {
		t.Fatalf("concurrent guest signups = %d", signups.Load())
	}
}

func TestBothPicsAnonymousSessionWaitHonorsCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan struct{})
		release := make(chan struct{})
		var first atomic.Bool
		client := bothPicsTestGuestClient(t, 1, func(request *http.Request) (*http.Response, error) {
			if request.URL.Path == "/screenshots/new" && !first.Swap(true) {
				close(started)
				<-release
			}
			return nil, nil
		})
		uploader := &bothPicsUploader{client: client}
		firstDone := make(chan error, 1)
		go func() {
			_, err := uploader.guestSession(t.Context())
			firstDone <- err
		}()
		<-started
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		secondDone := make(chan error, 1)
		go func() {
			_, err := uploader.guestSession(ctx)
			secondDone <- err
		}()
		synctest.Wait()
		cancel()
		if err := <-secondDone; !errors.Is(err, context.Canceled) {
			t.Fatalf("waiting for guest session cancellation: %v", err)
		}
		close(release)
		if err := <-firstDone; err != nil {
			t.Fatalf("first guest initialization failed: %v", err)
		}
	})
}

func TestBothPicsAnonymousSessionExpiresWithoutAutomaticUploadRetry(t *testing.T) {
	t.Parallel()
	paths := bothPicsTestPaths(t, 1)
	for _, expireCookie := range []bool{false, true} {
		t.Run(fmt.Sprintf("cookie_expired_%t", expireCookie), func(t *testing.T) {
			var signups atomic.Int32
			var rejectSession atomic.Bool
			client := bothPicsTestGuestClient(t, 1, func(request *http.Request) (*http.Response, error) {
				if request.URL.Path == "/guest" {
					signups.Add(1)
				}
				if request.URL.Path == "/app-api/collections" && rejectSession.Load() {
					return imageTestResponse(http.StatusUnauthorized, ""), nil
				}
				return nil, nil
			})
			uploader := &bothPicsUploader{client: client}
			if _, err := uploader.UploadBatch(t.Context(), paths); err != nil {
				t.Fatalf("initial anonymous upload: %v", err)
			}
			if expireCookie {
				uploader.guest.client.Jar.SetCookies(&url.URL{Scheme: "https", Host: "both.pics"}, []*http.Cookie{
					{
						Name:   "__Host-bp_session",
						Path:   "/",
						Secure: true,
						MaxAge: -1,
					},
				})
			} else {
				rejectSession.Store(true)
				_, err := uploader.UploadBatch(t.Context(), paths)
				if !errors.Is(err, errBothPicsGuestSessionExpired) || signups.Load() != 1 {
					t.Fatal("rejected guest session was retried automatically")
				}
				rejectSession.Store(false)
			}
			if _, err := uploader.UploadBatch(t.Context(), paths); err != nil {
				t.Fatalf("next explicitly requested anonymous upload: %v", err)
			}
			if signups.Load() != 2 {
				t.Fatalf("guest signups after expiry = %d, want 2", signups.Load())
			}
		})
	}
}

func TestBothPicsAnonymousFailuresDoNotCreateExtraSessions(t *testing.T) {
	t.Parallel()
	paths := bothPicsTestPaths(t, 1)
	scenarios := []struct {
		name   string
		method string
		path   string
		status int
		body   string
	}{
		{
			name:   "guest disabled redirect",
			method: http.MethodGet,
			path:   "/screenshots/new",
			status: http.StatusSeeOther,
		},
		{
			name:   "guest disabled forbidden",
			method: http.MethodPost,
			path:   "/guest",
			status: http.StatusForbidden,
		},
		{
			name:   "guest quota",
			method: http.MethodPost,
			path:   "/guest",
			status: http.StatusTooManyRequests,
		},
		{
			name:   "invalid creation JSON",
			method: http.MethodPost,
			path:   "/app-api/collections",
			status: http.StatusCreated,
			body:   `{"id":`,
		},
		{
			name:   "incomplete collection",
			method: http.MethodPost,
			path:   "/app-api/collections",
			status: http.StatusCreated,
			body:   `{}`,
		},
		{
			name:   "invalid image upload",
			method: http.MethodPut,
			path:   "/app-api/collections/" + bothPicsTestCollectionID + "/images/00000000-0000-4000-8000-000000000001",
			status: http.StatusBadRequest,
		},
		{
			name:   "finalization failure",
			method: http.MethodPost,
			path:   "/app-api/collections/" + bothPicsTestCollectionID + "/finalize",
			status: http.StatusInternalServerError,
		},
		{
			name:   "polling session expired",
			method: http.MethodGet,
			path:   "/app-api/collections/" + bothPicsTestCollectionID,
			status: http.StatusUnauthorized,
		},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			failed := false
			client := bothPicsTestGuestClient(t, 1, func(request *http.Request) (*http.Response, error) {
				if failed {
					t.Fatal("anonymous upload retried a terminal failure")
				}
				if request.Method == scenario.method && request.URL.Path == scenario.path {
					failed = true
					return imageTestResponse(scenario.status, scenario.body), nil
				}
				return nil, nil
			})
			_, err := (&bothPicsUploader{client: client}).UploadBatch(t.Context(), paths)
			if err == nil || !failed {
				t.Fatalf("anonymous failure = %v, reached expected failure = %t", err, failed)
			}
			if scenario.status == http.StatusSeeOther && !strings.Contains(err.Error(), "guest access") {
				t.Fatalf("anonymous disabled error is not actionable: %v", err)
			}
		})
	}
}

func TestBothPicsAnonymousCSRFAndCookiesAreRedacted(t *testing.T) {
	t.Parallel()
	paths := bothPicsTestPaths(t, 1)
	for _, failurePath := range []string{"/app-api/collections", "/app-api/collections/" + bothPicsTestCollectionID} {
		t.Run(failurePath, func(t *testing.T) {
			transportErr := errors.New("synthetic failure: " + bothPicsTestGuestCookie + " " + bothPicsTestDevice + " " + bothPicsTestCSRF)
			client := bothPicsTestGuestClient(t, 1, func(request *http.Request) (*http.Response, error) {
				if request.URL.Path == failurePath {
					return nil, transportErr
				}
				return nil, nil
			})
			_, err := (&bothPicsUploader{client: client}).UploadBatch(t.Context(), paths)
			if err == nil || !errors.Is(err, transportErr) {
				t.Fatal("anonymous transport failure did not preserve its cause")
			}
			for _, secret := range []string{bothPicsTestGuestCookie, bothPicsTestDevice, bothPicsTestCSRF} {
				if strings.Contains(err.Error(), secret) {
					t.Fatal("anonymous transport failure exposed session material")
				}
			}
		})
	}
}

func TestBothPicsAnonymousRejectsInvalidCSRF(t *testing.T) {
	t.Parallel()
	paths := bothPicsTestPaths(t, 1)
	for _, page := range []string{"<html>Signed out</html>", `<script id="csrf">{}</script>`, `<script id="csrf">""</script>`} {
		t.Run(page, func(t *testing.T) {
			client := bothPicsTestGuestClient(t, 1, func(request *http.Request) (*http.Response, error) {
				if request.URL.Path == "/screenshots/new" {
					if _, err := request.Cookie("__Host-bp_session"); err == nil {
						return imageTestResponse(http.StatusOK, page), nil
					}
				}
				if strings.HasPrefix(request.URL.Path, "/app-api/") {
					t.Fatal("invalid CSRF allowed collection creation")
				}
				return nil, nil
			})
			if _, err := (&bothPicsUploader{client: client}).UploadBatch(t.Context(), paths); err == nil {
				t.Fatal("invalid CSRF response succeeded")
			}
		})
	}
}

func TestBothPicsAnonymousRejectsInvalidUploadSlots(t *testing.T) {
	t.Parallel()
	paths := bothPicsTestPaths(t, 2)
	scenarios := []struct {
		name   string
		mutate func(*bothPicsTestCollection)
	}{
		{name: "unsafe collection ID", mutate: func(c *bothPicsTestCollection) { c.ID = "../unsafe" }},
		{name: "unsafe image ID", mutate: func(c *bothPicsTestCollection) { c.Images[0].ID = "../unsafe" }},
		{name: "duplicate image ID", mutate: func(c *bothPicsTestCollection) { c.Images[0].ID = c.Images[1].ID }},
		{name: "duplicate frame ID", mutate: func(c *bothPicsTestCollection) { c.Frames[0].ID = c.Frames[1].ID }},
		{name: "duplicate image frame", mutate: func(c *bothPicsTestCollection) { c.Images[0].FrameID = c.Images[1].FrameID }},
		{name: "missing image", mutate: func(c *bothPicsTestCollection) { c.Images = c.Images[:1] }},
		{name: "incorrect label", mutate: func(c *bothPicsTestCollection) { c.Frames[0].Label = "2" }},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			collection := newBothPicsTestCollection(2)
			scenario.mutate(&collection)
			created := false
			client := bothPicsTestGuestClient(t, 2, func(request *http.Request) (*http.Response, error) {
				if created {
					t.Fatal("invalid collection slots allowed an image upload")
				}
				if request.URL.Path == "/app-api/collections" {
					created = true
					return imageTestResponse(http.StatusCreated, bothPicsTestJSON(t, collection)), nil
				}
				return nil, nil
			})
			if _, err := (&bothPicsUploader{client: client}).UploadBatch(t.Context(), paths); err == nil || !created {
				t.Fatal("invalid anonymous collection slots succeeded")
			}
		})
	}
}
