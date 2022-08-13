package kvraft

import (
	"testing"
	"time"
)

func TestSf(t *testing.T) {
	init_snowflake()
	for i := 0; i < 100; i++ {
		got1 := make_snowflake()
		got2 := make_snowflake()
		got3 := make_snowflake()
		got4 := make_snowflake()
		got5 := make_snowflake()
		ts1 := get_timestamp_from_snowflake(got1)
		ts2 := get_timestamp_from_snowflake(got2)
		ts3 := get_timestamp_from_snowflake(got3)
		ts4 := get_timestamp_from_snowflake(got4)
		ts5 := get_timestamp_from_snowflake(got5)
		x1 := get_id_from_snowflake(got1)
		if ts1 == ts2 {
			x2 := get_id_from_snowflake(got2)
			if x2 != x1+1 {
				t.Errorf("id error %v %v %v,%v", x1, x2, ts1, ts2)
			}
		}
		if ts1 == ts3 {
			x3 := get_id_from_snowflake(got3)
			if x3 != x1+2 {
				t.Errorf("id error")
			}
		}
		if ts1 == ts4 {
			x4 := get_id_from_snowflake(got4)
			if x4 != x1+3 {
				t.Errorf("id error")
			}
		}
		if ts1 == ts5 {
			x5 := get_id_from_snowflake(got5)
			if x5 != x1+4 {
				t.Errorf("id error")
			}
		}

	}
	time.Sleep(2 * time.Millisecond)
	for i := 0; i < 100; i++ {
		got1 := make_snowflake()
		time.Sleep(2 * time.Millisecond)
		got2 := make_snowflake()
		x1 := get_id_from_snowflake(got1)
		x2 := get_id_from_snowflake(got2)
		if x1 != x2 {
			t.Errorf("id error %v %v", x1, x2)
		}
		time.Sleep(2 * time.Millisecond)

	}
}
