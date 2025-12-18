package keys

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKeys_Builders(t *testing.T) {
	q := "email"
	assert.Equal(t, "uniqw:{email}:pending", Pending(q))
	assert.Equal(t, "uniqw:{email}:active", Active(q))
	assert.Equal(t, "uniqw:{email}:delayed", Delayed(q))
	assert.Equal(t, "uniqw:{email}:dead", Dead(q))
	assert.Equal(t, "uniqw:{email}:succeeded", Succeeded(q))
	assert.Equal(t, "uniqw:{email}:unique", Unique(q))
	assert.Equal(t, "uniqw:{email}:expiry", Expiry(q))
}

func TestKeys_For(t *testing.T) {
	q := For("video")
	assert.Equal(t, "uniqw:{video}:pending", q.Pending)
	assert.Equal(t, "uniqw:{video}:active", q.Active)
	assert.Equal(t, "uniqw:{video}:delayed", q.Delayed)
	assert.Equal(t, "uniqw:{video}:dead", q.Dead)
	assert.Equal(t, "uniqw:{video}:succeeded", q.Succeeded)
	assert.Equal(t, "uniqw:{video}:unique", q.Unique)
	assert.Equal(t, "uniqw:{video}:expiry", q.Expiry)
}
