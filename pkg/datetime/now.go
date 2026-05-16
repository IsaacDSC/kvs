package datetime

import "time"

// Now returns the current instant; the default delegates to time.Now.
// Tests may replace it temporarily (avoid with t.Parallel).
var Now = time.Now
