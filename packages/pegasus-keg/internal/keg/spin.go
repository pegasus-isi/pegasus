package keg

import (
	"math/rand/v2"
	"time"
)

// fractal iterates z := z^2 + c from z = x+iy, c = a+ib until |z| >= 2 or
// max iterations, returning the iteration count.
func fractal(x, y, a, b float64, max int) int {
	qx, qy := x*x, y*y
	n := 0
	for ; n < max && qx+qy < 4.0; n++ {
		xx := qx - qy + a
		y = 2.0*x*y + b
		x = xx
		qx, qy = x*x, y*y
	}
	return n
}

// spin burns CPU computing random points of a random Julia set for d.
// It always does at least one round, and returns the number of rounds.
func spin(d time.Duration) int {
	stop := time.Now().Add(d)
	jx, jy := 1.0-2.0*rand.Float64(), 1.0-2.0*rand.Float64()
	count := 0
	for {
		for i := 0; i < 16; i++ {
			fractal(1.0-2.0*rand.Float64(), 1.0-2.0*rand.Float64(), jx, jy, 1024)
		}
		count++
		if !time.Now().Before(stop) {
			return count
		}
	}
}
