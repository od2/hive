package main

import (
	"encoding/hex"

	"github.com/spf13/viper"
	"go.od2.network/hive/pkg/token"
)

func getSigner() token.Signer {
	authSecret := new([32]byte)
	authSecretStr := viper.GetString(ConfAuthgwSecret)
	if len(authSecretStr) != 64 {
		log.Fatal("Invalid " + ConfAuthgwSecret)
	}
	if _, err := hex.Decode(authSecret[:], []byte(authSecretStr)); err != nil {
		log.Fatal("Invalid hex in " + ConfAuthgwSecret)
	}
	return token.NewSimpleSigner(authSecret)
}
