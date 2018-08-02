package raft

import "log"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func SearchFirst(left int, right int, predicate func(int) bool) int {
	if left > right || !predicate(left) {
		return -1
	}
	return SearchLast(left, right, func(x int) bool { return !predicate(x) }) + 1
}

func SearchLast(left int, right int, predicate func(int) bool) int {
	if left > right || !predicate(left) {
		return -1
	}

	// Linear search
	if right-left < 5 {
		ans := left
		for ans+1 <= right && predicate(ans+1) {
			ans++
		}
		return ans
	}

	// Binary search
	ans := -1
	for left <= right {
		middle := (left + right) / 2
		if predicate(middle) {
			ans = middle
			left = middle + 1
		} else {
			right = middle - 1
		}
	}
	return ans
}

func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func Max(x, y int) int {
	if x > y {
		return x
	}
	return y
}
