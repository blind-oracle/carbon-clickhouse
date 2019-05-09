package uploader

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Cmap(t *testing.T) {
	k := "foobar"
	m := NewCMap()

	m.Set(k, 0, 123)
	_, v := m.Get(k)
	assert.Equal(t, uint64(0x10000007b), v)
	m.Set(k, 1, 123)
	_, v = m.Get(k)
	assert.Equal(t, uint64(0x30000007b), v)
	m.Set(k, 2, 124)
	_, v = m.Get(k)
	assert.Equal(t, uint64(0x70000007c), v)

	assert.True(t, m.Exists(k, 0))
	assert.True(t, m.Exists(k, 1))
	assert.True(t, m.Exists(k, 2))
	assert.False(t, m.Exists(k, 3))
	assert.False(t, m.Exists("baz", 0))
}
