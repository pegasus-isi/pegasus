package handler

import "testing"

// TestRegistry_ContainerDispatch guards DockerHandler's and
// SingularityHandler's protocol maps: with main.go's registration order
// (docker first), the "::<type>" destination key the engine builds must still
// reach the right handler.
func TestRegistry_ContainerDispatch(t *testing.T) {
	reg := NewRegistry(NewDockerHandler(Hooks{}), NewSingularityHandler(Hooks{}))

	cases := []struct{ src, dst, want string }{
		{"docker", "file", "DockerHandler"},
		{"docker", "file::docker", "DockerHandler"},
		{"docker", "file::singularity", "SingularityHandler"},
		{"shub", "file::singularity", "SingularityHandler"},
		{"library", "file::singularity", "SingularityHandler"},
	}
	for _, tc := range cases {
		h := reg.Find(tc.src, tc.dst)
		if h == nil {
			t.Errorf("Find(%q, %q) = nil, want %s", tc.src, tc.dst, tc.want)
			continue
		}
		if h.Name() != tc.want {
			t.Errorf("Find(%q, %q) = %s, want %s", tc.src, tc.dst, h.Name(), tc.want)
		}
	}
}
