package s3

import (
	"testing"
	"time"
)

func Test_semaphore_Use(t *testing.T) {
	type testcase struct {
		name string
		s    semaphore

		numberOfRuns int
		wantDuration time.Duration
	}

	tests := []testcase{
		{
			name:         "small limit",
			s:            makeSemaphore(50),
			numberOfRuns: 50,
			wantDuration: time.Second,
		},
		{
			name:         "large limit",
			s:            makeSemaphore(150),
			numberOfRuns: 150,
			wantDuration: time.Second,
		},
		{
			name:         "unset",
			s:            nil,
			numberOfRuns: 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start := time.Now()
			for i := 0; i < tt.numberOfRuns; i++ {
				tt.s.Use()
			}

			end := time.Now()
			got := end.Sub(start)
			if got < tt.wantDuration {
				t.Fatalf("invalid time, expected at least %v and received %v", tt.wantDuration, got)
			}
		})
	}
}
