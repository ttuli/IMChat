package appversion

import "testing"

func TestParse(t *testing.T) {
	cases := []struct {
		in   string
		want Version
		ok   bool
	}{
		{"1.2.3", Version{1, 2, 3}, true},
		{" v1.2.3 ", Version{1, 2, 3}, true},
		{"1.2.3-beta.1", Version{1, 2, 3}, true},
		{"1.2.3+build.5", Version{1, 2, 3}, true},
		{"10.0.12", Version{10, 0, 12}, true},
		{"", Version{}, false},
		{"1.2", Version{}, false},
		{"1.2.3.4", Version{}, false},
		{"1.x.3", Version{}, false},
		{"1.-2.3", Version{}, false},
		{"1.+2.3", Version{}, false},
		{"1..3", Version{}, false},
	}
	for _, c := range cases {
		got, ok := Parse(c.in)
		if ok != c.ok || got != c.want {
			t.Errorf("Parse(%q) = %v, %v; want %v, %v", c.in, got, ok, c.want, c.ok)
		}
	}
}

func TestCompare(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"1.2.3", "1.2.3", 0},
		{"1.2.3", "1.2.4", -1},
		{"1.10.0", "1.9.9", 1}, // 按数值而非字符串比较
		{"2.0.0", "1.99.99", 1},
		{"0.0.0", "1.0.0", -1},
	}
	for _, c := range cases {
		a, _ := Parse(c.a)
		b, _ := Parse(c.b)
		if got := a.Compare(b); got != c.want {
			t.Errorf("%s vs %s = %d; want %d", c.a, c.b, got, c.want)
		}
	}
}
