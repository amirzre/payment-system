package domain

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

// TokenClaims represents the JWT claims for an access token.
type TokenClaims struct {
	jwt.RegisteredClaims
	UserID string `json:"user_id"`
}

// TokenPair holds an access token and a refresh token.
type TokenPair struct {
	AccessToken  string
	RefreshToken string
	ExpiresIn    int64
}

// TokenService handles JWT issuance and verification.
type TokenService struct {
	privateKey *rsa.PrivateKey
	publicKey  *rsa.PublicKey
}

// NewTokenService creates a new TokenService.
func NewTokenService(privateKeyPEM, publicKeyPEM string) (*TokenService, error) {
	var privateKey *rsa.PrivateKey
	var publicKey *rsa.PublicKey

	if privateKeyPEM != "" {
		block, _ := pem.Decode([]byte(privateKeyPEM))
		if block == nil {
			return nil, fmt.Errorf("failed to decode private key PEM")
		}
		key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parsing private key: %w", err)
		}
		var ok bool
		privateKey, ok = key.(*rsa.PrivateKey)
		if !ok {
			return nil, fmt.Errorf("private key is not RSA")
		}
		publicKey = &privateKey.PublicKey
	} else if publicKeyPEM != "" {
		block, _ := pem.Decode([]byte(publicKeyPEM))
		if block == nil {
			return nil, fmt.Errorf("failed to decode public key PEM")
		}
		key, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parsing public key: %w", err)
		}
		var ok bool
		publicKey, ok = key.(*rsa.PublicKey)
		if !ok {
			return nil, fmt.Errorf("public key is not RSA")
		}
	} else {
		key, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			return nil, fmt.Errorf("generating RSA key: %w", err)
		}
		privateKey = key
		publicKey = &key.PublicKey
		fmt.Fprintln(os.Stderr, "WARNING: Using auto-generated RSA key pair (development only)")
	}

	return &TokenService{
		privateKey: privateKey,
		publicKey:  publicKey,
	}, nil
}

// IssueToken creates a new access/refresh token pair for the given user.
func (s *TokenService) IssueToken(userID uuid.UUID) (*TokenPair, error) {
	if s.privateKey == nil {
		return nil, fmt.Errorf("cannot issue tokens: no private key configured")
	}

	now := time.Now().UTC()
	accessExpiry := now.Add(15 * time.Minute)
	refreshExpiry := now.Add(7 * 24 * time.Hour)

	accessClaims := TokenClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   userID.String(),
			ExpiresAt: jwt.NewNumericDate(accessExpiry),
			IssuedAt:  jwt.NewNumericDate(now),
			ID:        uuid.NewString(),
		},
		UserID: userID.String(),
	}
	accessToken := jwt.NewWithClaims(jwt.SigningMethodRS256, accessClaims)
	accessSigned, err := accessToken.SignedString(s.privateKey)
	if err != nil {
		return nil, fmt.Errorf("signing access token: %w", err)
	}

	refreshClaims := jwt.RegisteredClaims{
		Subject:   userID.String(),
		ExpiresAt: jwt.NewNumericDate(refreshExpiry),
		IssuedAt:  jwt.NewNumericDate(now),
		ID:        uuid.NewString(),
	}
	refreshToken := jwt.NewWithClaims(jwt.SigningMethodRS256, refreshClaims)
	refreshSigned, err := refreshToken.SignedString(s.privateKey)
	if err != nil {
		return nil, fmt.Errorf("signing refresh token: %w", err)
	}

	return &TokenPair{
		AccessToken:  accessSigned,
		RefreshToken: refreshSigned,
		ExpiresIn:    int64(15 * 60),
	}, nil
}

// VerifyToken validates a JWT and returns the user ID if valid.
func (s *TokenService) VerifyToken(tokenString string) (bool, string) {
	token, err := jwt.ParseWithClaims(tokenString, &TokenClaims{}, func(token *jwt.Token) (any, error) {
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return s.publicKey, nil
	})
	if err != nil {
		return false, ""
	}

	claims, ok := token.Claims.(*TokenClaims)
	if !ok || !token.Valid {
		return false, ""
	}

	return true, claims.UserID
}
