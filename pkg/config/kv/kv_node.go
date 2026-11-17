package kv

import (
	"cmp"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/kvtools/valkeyrie/store"
	"github.com/traefik/paerser/parser"
)

// DecodeToNode converts the labels to a tree of nodes.
// If any filters are present, labels which do not match the filters are skipped.
func DecodeToNode(pairs []*store.KVPair, rootName string, filters ...string) (*parser.Node, error) {
	sortedPairs := filterPairs(pairs, filters)

	exp := regexp.MustCompile(`^\d+$`)

	var node *parser.Node

	for i, pair := range sortedPairs {
		if !strings.HasPrefix(pair.Key, rootName+"/") {
			return nil, fmt.Errorf("invalid label root %s", rootName)
		}

		split := strings.FieldsFunc(pair.Key[len(rootName)+1:], func(c rune) bool { return c == '/' })

		parts := []string{rootName}
		for _, fragment := range split {
			if exp.MatchString(fragment) {
				parts = append(parts, "["+fragment+"]")
			} else {
				parts = append(parts, fragment)
			}
		}

		if i == 0 {
			node = &parser.Node{}
		}
		decodeToNode(node, parts, string(pair.Value))
	}

	return node, nil
}

func decodeToNode(root *parser.Node, path []string, value string) {
	if len(root.Name) == 0 {
		root.Name = path[0]
	}

	// it's a leaf or not -> children
	if len(path) > 1 {
		if n := containsNode(root.Children, path[1]); n != nil {
			// the child already exists
			decodeToNode(n, path[1:], value)
		} else {
			// new child
			child := &parser.Node{Name: path[1]}
			decodeToNode(child, path[1:], value)
			root.Children = append(root.Children, child)
		}
	} else {
		root.Value = value
	}
}

func containsNode(nodes []*parser.Node, name string) *parser.Node {
	for _, n := range nodes {
		if strings.EqualFold(name, n.Name) {
			return n
		}
	}
	return nil
}

func filterPairs(pairs []*store.KVPair, filters []string) []*store.KVPair {
	exp := regexp.MustCompile(`^(.+)/\d+$`)

	pairs = slices.Clone(pairs)
	sortKVPairs(pairs)

	simplePairs := map[string]*store.KVPair{}
	slicePairs := map[string][]string{}

	for _, pair := range pairs {
		if len(filters) == 0 {
			// Slice of simple type
			if exp.MatchString(pair.Key) {
				sanitizedKey := exp.FindStringSubmatch(pair.Key)[1]
				slicePairs[sanitizedKey] = append(slicePairs[sanitizedKey], string(pair.Value))
			} else {
				simplePairs[pair.Key] = pair
			}
			continue
		}

		for _, filter := range filters {
			if len(pair.Key) >= len(filter) && strings.EqualFold(pair.Key[:len(filter)], filter) {
				// Slice of simple type
				if exp.MatchString(pair.Key) {
					sanitizedKey := exp.FindStringSubmatch(pair.Key)[1]
					slicePairs[sanitizedKey] = append(slicePairs[sanitizedKey], string(pair.Value))
				} else {
					simplePairs[pair.Key] = pair
				}
				continue
			}
		}
	}

	var sortedPairs []*store.KVPair
	for k, v := range slicePairs {
		delete(simplePairs, k)
		sortedPairs = append(sortedPairs, &store.KVPair{Key: k, Value: []byte(strings.Join(v, ","))})
	}

	for _, v := range simplePairs {
		sortedPairs = append(sortedPairs, v)
	}

	sortKVPairs(sortedPairs)

	return sortedPairs
}

func sortKVPairs(pairs []*store.KVPair) {
	slices.SortFunc(pairs, func(left, right *store.KVPair) int {
		return compareKVKey(left.Key, right.Key)
	})
}

func compareKVKey(left, right string) int {
	commonPrefix := 0
	for commonPrefix < min(len(left), len(right)) && left[commonPrefix] == right[commonPrefix] {
		commonPrefix++
	}

	if commonPrefix == min(len(left), len(right)) {
		return cmp.Compare(len(left), len(right))
	}

	if isDecimalDigit(left[commonPrefix]) && isDecimalDigit(right[commonPrefix]) {
		segmentStart := strings.LastIndexByte(left[:commonPrefix], '/') + 1
		leftSegment := segmentAt(left, segmentStart)
		rightSegment := segmentAt(right, segmentStart)

		if isDecimal(leftSegment) && isDecimal(rightSegment) {
			// Indexes are part of the configuration structure, so compare them numerically before falling back to their
			// textual representation to keep the result deterministic for duplicate indexes such as 1 and 01.
			if result := compareDecimal(leftSegment, rightSegment); result != 0 {
				return result
			}
		}
	}

	return cmp.Compare(left, right)
}

func segmentAt(key string, start int) string {
	index := strings.IndexByte(key[start:], '/')
	if index < 0 {
		return key[start:]
	}

	return key[start : start+index]
}

func compareDecimal(left, right string) int {
	left = strings.TrimLeft(left, "0")
	right = strings.TrimLeft(right, "0")

	if left == "" {
		left = "0"
	}
	if right == "" {
		right = "0"
	}

	if result := cmp.Compare(len(left), len(right)); result != 0 {
		return result
	}

	return cmp.Compare(left, right)
}

func isDecimal(value string) bool {
	if value == "" {
		return false
	}

	for i := range len(value) {
		if !isDecimalDigit(value[i]) {
			return false
		}
	}

	return true
}

func isDecimalDigit(value byte) bool {
	return value >= '0' && value <= '9'
}
