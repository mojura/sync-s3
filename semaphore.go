package s3

import "time"

func makeSemaphore(ratePerSecond int64) (s semaphore) {
	s = make(semaphore)
	go s.drain(ratePerSecond)
	return
}

type semaphore chan struct{}

func (s semaphore) Use() {
	if s == nil {
		return
	}

	s <- struct{}{}
}

func (s semaphore) drain(ratePerSecond int64) {
	for {
		select {
		case <-s:
		default:
		}

		time.Sleep(time.Second / time.Duration(ratePerSecond))
	}
}
