package topology

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCollectionTopic(t *testing.T) {
	assert.Equal(t, "yt.videos.tasks", CollectionTopic("yt.videos", "tasks"))
}

func TestCollectionOfTopic(t *testing.T) {
	assert.Equal(t, "yt.videos", CollectionOfTopic("yt.videos.tasks"))
	assert.Equal(t, "", CollectionOfTopic(""))
	assert.Equal(t, "", CollectionOfTopic("."))
	assert.Equal(t, "", CollectionOfTopic(".items"))
	assert.Equal(t, "", CollectionOfTopic("items"))
	assert.Equal(t, "yt.videos", CollectionOfTopic("yt.videos."))
}
