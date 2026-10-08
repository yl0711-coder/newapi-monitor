package monitor

import (
	"os"
	"strings"
)

// BuildGitSHA and BuildImageVersion are populated by the image build with
// -ldflags.  Keeping a runtime fallback is useful for local go run/dev builds,
// but the /ready response always exposes the value that the process actually
// has rather than trying to infer it from a mutable tag.
var (
	BuildGitSHA       = "unknown"
	BuildImageVersion = "unknown"
)

func effectiveBuildMetadata() (gitSHA, imageVersion string) {
	gitSHA = strings.TrimSpace(BuildGitSHA)
	if gitSHA == "" || gitSHA == "unknown" {
		if v := strings.TrimSpace(os.Getenv("MONITOR_GIT_SHA")); v != "" {
			gitSHA = v
		}
	}
	if gitSHA == "" {
		gitSHA = "unknown"
	}

	imageVersion = strings.TrimSpace(BuildImageVersion)
	if imageVersion == "" || imageVersion == "unknown" {
		if v := strings.TrimSpace(os.Getenv("MONITOR_IMAGE_VERSION")); v != "" {
			imageVersion = v
		} else if v := strings.TrimSpace(os.Getenv("MONITOR_IMAGE")); v != "" {
			// MONITOR_IMAGE is commonly an immutable tag or digest in ECS/local
			// compose.  Expose it as a fallback, without parsing or rewriting it.
			imageVersion = v
		}
	}
	if imageVersion == "" {
		imageVersion = "unknown"
	}
	return gitSHA, imageVersion
}
