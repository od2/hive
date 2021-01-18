package auth

// GitHubUser contains minimal identity claims.
type GitHubUser struct {
	ID    int64  `json:"id"`
	Login string `json:"login"`
}
