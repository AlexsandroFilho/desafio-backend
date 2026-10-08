package middleware

import "testing"

func TestResolveProviderID(t *testing.T) {
	tests := []struct {
		name      string
		current   string
		legacy    string
		want      string
		wantError bool
	}{
		{
			name:    "current claim",
			current: " provider-a ",
			want:    "provider-a",
		},
		{
			name:   "legacy claim",
			legacy: "provider-legacy",
			want:   "provider-legacy",
		},
		{
			name:    "matching claims",
			current: "provider-a",
			legacy:  "provider-a",
			want:    "provider-a",
		},
		{
			name:      "conflicting claims",
			current:   "provider-a",
			legacy:    "provider-b",
			wantError: true,
		},
		{
			name:      "missing claims",
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveProviderID(tt.current, tt.legacy)
			if (err != nil) != tt.wantError {
				t.Fatalf("resolveProviderID() error = %v, wantError %v", err, tt.wantError)
			}
			if got != tt.want {
				t.Errorf("resolveProviderID() = %q, want %q", got, tt.want)
			}
		})
	}
}
