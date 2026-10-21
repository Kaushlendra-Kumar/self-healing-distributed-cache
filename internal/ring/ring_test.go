package ring

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"testing/quick"
)

func nodes(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("node-%d", i+1)
	}
	return out
}

func keys(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("user:%d:profile", i)
	}
	return out
}

func TestDeterministicAcrossInstancesAndOrder(t *testing.T) {
	a := New(64, []string{"a", "b", "c"})
	b := New(64, []string{"c", "a", "b", "a", ""}) // different order, dupes, empty
	for _, k := range keys(2000) {
		oa, ob := a.Owners(k, 2), b.Owners(k, 2)
		if fmt.Sprint(oa) != fmt.Sprint(ob) {
			t.Fatalf("key %s: %v != %v", k, oa, ob)
		}
	}
	if b.Len() != 3 || !b.Contains("c") || b.Contains("") {
		t.Fatal("membership")
	}
}

func TestOwnersDistinctAndBounded(t *testing.T) {
	r := New(32, nodes(5))
	f := func(key string, n uint8) bool {
		want := int(n % 8)
		o := r.Owners(key, want)
		if len(o) != min(want, 5) {
			return false
		}
		seen := map[string]bool{}
		for _, x := range o {
			if seen[x] {
				return false
			}
			seen[x] = true
		}
		return len(o) == 0 || o[0] == r.Primary(key)
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 2000}); err != nil {
		t.Fatal(err)
	}
}

func TestEmptyRing(t *testing.T) {
	var r Ring
	if r.Owners("k", 3) != nil || r.Primary("k") != "" {
		t.Fatal("empty ring must own nothing")
	}
	if New(0, nil).Len() != 0 {
		t.Fatal("len")
	}
}

// remapRatio is the fraction of keys whose primary changed.
func remapRatio(before, after func(string) string, ks []string) float64 {
	moved := 0
	for _, k := range ks {
		if before(k) != after(k) {
			moved++
		}
	}
	return float64(moved) / float64(len(ks))
}

func TestAddingNodeMovesFewKeysComparedToModulo(t *testing.T) {
	ks := keys(50_000)
	r4, r5 := New(128, nodes(4)), New(128, nodes(5))
	ring := remapRatio(r4.Primary, r5.Primary, ks)

	mod := func(n int) func(string) string {
		return func(k string) string { return fmt.Sprint(Hash(k) % uint64(n)) }
	}
	modulo := remapRatio(mod(4), mod(5), ks)

	t.Logf("4->5 nodes: consistent hashing moved %.1f%% of keys, modulo hashing moved %.1f%%", ring*100, modulo*100)
	// Ideal for consistent hashing is 1/5 = 20%; modulo moves ~80%.
	if ring > 0.30 {
		t.Fatalf("consistent hashing moved too many keys: %.3f", ring)
	}
	if modulo < 0.70 {
		t.Fatalf("modulo baseline unexpectedly low: %.3f", modulo)
	}
}

func TestRemovingNodeOnlyMovesItsKeys(t *testing.T) {
	ks := keys(20_000)
	full := New(64, nodes(5))
	without := New(64, []string{"node-1", "node-2", "node-4", "node-5"})
	for _, k := range ks {
		before := full.Primary(k)
		after := without.Primary(k)
		if before != "node-3" && before != after {
			t.Fatalf("key %s moved from %s to %s although %s was not removed", k, before, after, before)
		}
		if after == "node-3" {
			t.Fatal("removed node still owns keys")
		}
		// Replica sets also shift minimally: surviving owners keep their order.
		ob := full.Owners(k, 3)
		oa := without.Owners(k, 3)
		var kept []string
		for _, o := range ob {
			if o != "node-3" {
				kept = append(kept, o)
			}
		}
		for i := range kept {
			if oa[i] != kept[i] {
				t.Fatalf("key %s: surviving owner order changed %v -> %v", k, ob, oa)
			}
		}
	}
}

func stddevOfShares(r *Ring, ks []string) (counts map[string]int, relStd float64) {
	counts = map[string]int{}
	for _, k := range ks {
		counts[r.Primary(k)]++
	}
	mean := float64(len(ks)) / float64(r.Len())
	var ss float64
	for _, c := range counts {
		d := float64(c) - mean
		ss += d * d
	}
	return counts, math.Sqrt(ss/float64(r.Len())) / mean
}

func TestVirtualNodesImproveBalance(t *testing.T) {
	ks := keys(100_000)
	_, one := stddevOfShares(New(1, nodes(5)), ks)
	_, many := stddevOfShares(New(256, nodes(5)), ks)
	t.Logf("relative stddev of keys per node: 1 vnode=%.3f, 256 vnodes=%.3f", one, many)
	if many >= one {
		t.Fatal("virtual nodes did not improve balance")
	}
	if many > 0.15 {
		t.Fatalf("256 vnodes still unbalanced: %.3f", many)
	}
}

func TestOwnershipFractionsSumToOne(t *testing.T) {
	for _, vn := range []int{1, 16, 128} {
		fr := New(vn, nodes(4)).OwnershipFractions()
		var sum float64
		for _, f := range fr {
			sum += f
		}
		if math.Abs(sum-1) > 1e-9 {
			t.Fatalf("vnodes=%d fractions sum to %f", vn, sum)
		}
	}
	if New(1, []string{"solo"}).OwnershipFractions()["solo"] != 1 {
		t.Fatal("single point must own everything")
	}
}

func TestHashSpread(t *testing.T) {
	// Similar inputs must not cluster: check the top bit is roughly uniform.
	ones := 0
	for i := 0; i < 10_000; i++ {
		if Hash(fmt.Sprintf("node-1#%d", i))>>63 == 1 {
			ones++
		}
	}
	if ones < 4700 || ones > 5300 {
		t.Fatalf("poor hash spread: %d/10000 high bits set", ones)
	}
}

func BenchmarkOwners(b *testing.B) {
	r := New(128, nodes(10))
	ks := keys(1024)
	rng := rand.New(rand.NewSource(1))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Owners(ks[rng.Intn(len(ks))], 3)
	}
}
