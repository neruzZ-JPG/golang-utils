package stringutil

import "testing"

func TestReverse(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "ascii", input: "golang", want: "gnalog"},
		{name: "unicode", input: "你好世界", want: "界世好你"},
		{name: "empty", input: "", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Reverse(tt.input); got != tt.want {
				t.Fatalf("Reverse(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}
