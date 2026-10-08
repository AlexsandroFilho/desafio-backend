package middleware

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/coreos/go-oidc/v3/oidc"
)

type providerIDContextKey struct{}

type AuthMiddleware struct {
	verifier *oidc.IDTokenVerifier
}

func NewAuthMiddleware(
	ctx context.Context,
	issuerURL string,
	audience string,
) (*AuthMiddleware, error) {
	provider, err := oidc.NewProvider(ctx, issuerURL)
	if err != nil {
		return nil, err
	}

	return &AuthMiddleware{
		verifier: provider.Verifier(&oidc.Config{ClientID: audience}),
	}, nil
}

func (m *AuthMiddleware) Authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rawToken, err := bearerToken(r.Header.Get("Authorization"))
		if err != nil {
			writeAuthError(w, http.StatusUnauthorized, "missing or invalid bearer token")
			return
		}

		idToken, err := m.verifier.Verify(r.Context(), rawToken)
		if err != nil {
			writeAuthError(w, http.StatusUnauthorized, "invalid or expired bearer token")
			return
		}

		var claims struct {
			ProviderID       string `json:"providerId"`
			ProviderIDLegacy string `json:"provider_id"`
		}
		if err := idToken.Claims(&claims); err != nil {
			writeAuthError(w, http.StatusUnauthorized, "invalid token claims")
			return
		}

		providerID, err := resolveProviderID(
			claims.ProviderID,
			claims.ProviderIDLegacy,
		)
		if err != nil {
			writeAuthError(w, http.StatusForbidden, err.Error())
			return
		}

		ctx := context.WithValue(r.Context(), providerIDContextKey{}, providerID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func resolveProviderID(providerID, legacyProviderID string) (string, error) {
	providerID = strings.TrimSpace(providerID)
	legacyProviderID = strings.TrimSpace(legacyProviderID)
	if providerID != "" &&
		legacyProviderID != "" &&
		providerID != legacyProviderID {
		return "", errors.New("conflicting providerId claims")
	}
	if providerID != "" {
		return providerID, nil
	}
	if legacyProviderID != "" {
		return legacyProviderID, nil
	}
	return "", errors.New("providerId claim is required")
}

func ProviderIDFromContext(ctx context.Context) (string, bool) {
	providerID, ok := ctx.Value(providerIDContextKey{}).(string)
	return providerID, ok && providerID != ""
}

func bearerToken(authorization string) (string, error) {
	parts := strings.Fields(authorization)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || parts[1] == "" {
		return "", errors.New("invalid authorization header")
	}
	return parts[1], nil
}

func writeAuthError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	if status == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", "Bearer")
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}
