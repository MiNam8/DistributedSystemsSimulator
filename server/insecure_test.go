package server_test

import (
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func insecureCreds() credentials.TransportCredentials { return insecure.NewCredentials() }
