// Copyright 2013 Alexandre Fiori
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package redis

import (
	"math/rand"
	"testing"
	"time"
)

// TODO: sort tests by dependency (set first, etc)

// rc is the redis client handler used for all tests.
// Make sure redis-server is running before starting the tests.

func init() {
	rc = New("127.0.0.1:6379", "127.0.0.1:6380", "127.0.0.1:6381")
	rand.Seed(time.Now().UTC().UnixNano())
}

// TestAppend appends " World" to "Hello" and expects the lenght to be 11.
func TestMultipleKeys(t *testing.T) {
	defer func() {
		rc.Del("foobar")
		rc.Del("jas")
		rc.Del("theclash")
	}()

	if err := rc.Set("foobar", "Hello"); err != nil {
		t.Error(err)
		return
	}

	if err := rc.Set("jas", "Hello"); err != nil {
		t.Error(err)
		return
	}

	if err := rc.Set("theclash", "Hello"); err != nil {
		t.Error(err)
		return
	}

	if v, err := rc.Get("foobar"); err != nil {
		t.Error(err)
	} else if v != "Hello" {
		t.Error("foobar")
	}

	if v, err := rc.Get("jas"); err != nil {
		t.Error(err)
	} else if v != "Hello" {
		t.Error("jas")
	}

	if v, err := rc.Get("theclash"); err != nil {
		t.Error(err)
	} else if v != "Hello" {
		t.Error("theclash")
	}
}
