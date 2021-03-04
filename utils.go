package s3

import "fmt"

// generateFilename will generate a Mojura filename from a given prefix and created at
func generateFilename(prefix string, createdAt int64) string {
	return fmt.Sprintf("%s%d.moj", prefix, createdAt)
}
