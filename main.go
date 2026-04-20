package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

func main() {
	bucket := mustEnv("S3_BUCKET")
	endpoint := os.Getenv("S3_ENDPOINT")
	region := os.Getenv("S3_REGION")
	if region == "" {
		region = "us-east-1"
	}
	pathPrefix := os.Getenv("PATH_PREFIX")
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	cacheMaxAge := os.Getenv("CACHE_MAX_AGE")
	if cacheMaxAge == "" {
		cacheMaxAge = "86400"
	}

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			mustEnv("S3_ACCESS_KEY"),
			mustEnv("S3_SECRET_KEY"),
			"",
		)),
		config.WithRegion(region),
	)
	if err != nil {
		log.Fatal(err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		}
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/", makeHandler(client, bucket, pathPrefix, cacheMaxAge))

	log.Printf("listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

func makeHandler(client *s3.Client, bucket, pathPrefix, cacheMaxAge string) http.HandlerFunc {
	cacheHeader := fmt.Sprintf("public, max-age=%s", cacheMaxAge)

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		key := strings.TrimPrefix(r.URL.Path, "/")
		if pathPrefix != "" {
			key = strings.TrimPrefix(key, strings.Trim(pathPrefix, "/")+"/")
		}
		if key == "" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}

		result, err := client.GetObject(r.Context(), &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			var apiErr smithy.APIError
			if errors.As(err, &apiErr) {
				code := apiErr.ErrorCode()
				if code == "NoSuchKey" || code == "404" || code == "AccessDenied" {
					http.Error(w, "not found", http.StatusNotFound)
					return
				}
			}
			log.Printf("s3 error for key %q: %v", key, err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		defer result.Body.Close()

		if result.ContentType != nil {
			w.Header().Set("Content-Type", *result.ContentType)
		}
		if result.ContentLength != nil && *result.ContentLength > 0 {
			w.Header().Set("Content-Length", fmt.Sprintf("%d", *result.ContentLength))
		}
		if result.ETag != nil {
			w.Header().Set("ETag", *result.ETag)
		}
		if result.LastModified != nil {
			w.Header().Set("Last-Modified", result.LastModified.UTC().Format(http.TimeFormat))
		}
		w.Header().Set("Cache-Control", cacheHeader)

		if r.Method == http.MethodHead {
			return
		}

		if _, err := io.Copy(w, result.Body); err != nil {
			log.Printf("stream error for key %q: %v", key, err)
		}
	}
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("missing required env var: %s", key)
	}
	return v
}
