package main

import "testing"

func Test_zfsDatasetEscape(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "Passes through valid characters",
			args: args{s: "this_is-Valid10"},
			want: "this_is-Valid10",
		},
		{
			name: "Escapes a simple character",
			args: args{s: "this is N!ÖT valid"},
			want: "this:20is:20N:21:C3:96T:20valid",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := zfsDatasetEscape(tt.args.s); got != tt.want {
				t.Errorf("zfsDatasetEscape() = %v, want %v", got, tt.want)
			}
		})
	}
}
