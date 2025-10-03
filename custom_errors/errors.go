package custom_errors

import (
	"errors"
)

var ErrQueueEmpty = errors.New("queue is empty")
