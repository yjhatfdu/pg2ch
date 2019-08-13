package pgutils

import (
	"bytes"
	"testing"
)

var testData = []struct {
	istore   []byte
	expected []byte
}{
	{
		istore:   []byte{},
		expected: []byte("[]\t[]"),
	},
	{
		istore:   []byte(`"0"=>"2", "1"=>"1", "6"=>"1", "7"=>"1", "8"=>"2", "9"=>"1", "10"=>"2", "11"=>"1", "12"=>"1", "13"=>"1", "14"=>"2", "15"=>"2", "16"=>"3", "17"=>"2", "18"=>"3", "19"=>"1", "20"=>"1", "21"=>"2", "22"=>"2"`),
		expected: []byte("[0,1,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22]\t[2,1,1,1,2,1,2,1,1,1,2,2,3,2,3,1,1,2,2]"),
	},
	{
		istore:   []byte(`"0"=>"2"`),
		expected: []byte("[0]\t[2]"),
	},
	{
		istore:   []byte(`"-1"=>"-2"`),
		expected: []byte("[-1]\t[-2]"),
	},
}

func TestIstoreToArrays(t *testing.T) {
	for i, tt := range testData {
		got := IstoreToArrays(tt.istore)
		if bytes.Compare(got, tt.expected) != 0 {
			t.Fatalf("%d: Expected: %v, got: %v", i, tt.expected, got)
		}
	}
}

func BenchmarkIstoreToArrays(b *testing.B) {
	for n := 0; n < b.N; n++ {
		IstoreToArrays(testData[1].istore)
	}
}