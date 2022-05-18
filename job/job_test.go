package job

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHellow(t *testing.T) {
	assert.Equal(t, "hellow taro!", HellowWorld("taro"), "assertion test")
}
