package redshiftdatasetannotator

import "strings"

func isProvisoned(hostname string) bool {
	return strings.HasSuffix(hostname, "redshift.amazonaws.com")
}

func getCluseterID(hostname string) string {
	if isProvisoned(hostname) {
		return strings.Split(hostname, ".")[0]
	}
	return ""
}

func isServeless(hostname string) bool {
	return strings.HasSuffix(hostname, "redshift-serverless.amazonaws.com")
}

func getWorkgroupName(hostname string) string {
	if isServeless(hostname) {
		return strings.Split(hostname, ".")[0]
	}
	return ""
}

func coalesce[T any](ptrs ...*T) T {
	for _, ptr := range ptrs {
		if ptr != nil {
			return *ptr
		}
	}
	var empty T
	return empty
}

func nillif[T comparable](t, empty T) *T {
	if t == empty {
		return nil
	}
	return &t
}

func clonePointer[T any](p *T) *T {
	if p == nil {
		return nil
	}
	cloned := *p
	return &cloned
}

func cloneSlice[T any](s []T) []T {
	if s == nil {
		return nil
	}
	cloned := make([]T, len(s))
	copy(cloned, s)
	return cloned
}

func cloneMap[T any](m map[string]T) map[string]T {
	if m == nil {
		return nil
	}
	cloned := make(map[string]T, len(m))
	for key, value := range m {
		cloned[key] = value
	}
	return cloned
}
