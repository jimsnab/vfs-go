package vfs

import (
	"encoding/binary"
	"fmt"
	"math"
	mrand "math/rand"
	"testing"

	"github.com/jimsnab/afero"
)

type (
	testState struct {
		originalFs afero.Fs
		tree       *avlTree
		shard      uint64
		position   uint64
		testDir    string
	}
)

func testInitialize(t *testing.T, makeAvlTree bool) (ts *testState) {
	ts = &testState{}
	ts.originalFs = AppFs
	AppFs = afero.NewMemMapFs()

	ts.testDir = "/afero/data"
	err := AppFs.MkdirAll(ts.testDir, 0744)
	if err != nil {
		t.Fatal(err)
	}

	if makeAvlTree {
		ts.tree, err = newAvlTree(&VfsConfig{IndexDir: ts.testDir, BaseName: "test"}, kTestKeyGroup, "dt1")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			ts.tree.Close()
		})
	}

	ts.shard = mrand.Uint64()
	ts.position = mrand.Uint64()

	t.Cleanup(func() {
		AppFs = ts.originalFs
	})
	return
}

func testKey(n float64) []byte {
	key := make([]byte, 8)
	u := math.Float64bits(n)
	binary.BigEndian.PutUint64(key, u)
	return key
}

func testGetRoot(tree *avlTree) float64 {
	root, err := tree.loadNode(tree.getRootOffset())
	if err != nil {
		return math.MaxFloat64
	}
	u := binary.BigEndian.Uint64(root.Key()[:8])
	return math.Float64frombits(u)
}

func testKeyInt(n int) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(n))
	return key
}

func (tree *avlTree) testIsValid(t *testing.T) bool {
	valid, err := tree.isValid()
	if err != nil {
		t.Fatal(err)
	}
	return valid
}

func (tree *avlTree) testRootNode(t *testing.T) *avlNode {
	root, err := tree.loadNode(tree.getRootOffset())
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func (tree *avlTree) testCountEach(t *testing.T) (count int) {
	count, err := tree.countEach()
	if err != nil {
		t.Fatal(err)
	}
	return
}

func (tree *avlTree) testRequireKey(t *testing.T, v float64) {
	node, err := tree.Find(testKey(v))
	if err != nil {
		t.Fatal(err)
	}
	if node == nil {
		t.Fatalf("can't find %v", v)
	}
}

func (tree *avlTree) testRequireNoKey(t *testing.T, v float64) {
	node, err := tree.Find(testKey(v))
	if err != nil {
		t.Fatal(err)
	}
	if node != nil {
		t.Fatalf("found %v", v)
	}
}

func TestAvlInsertLL(t *testing.T) {
	ts := testInitialize(t, true)
	tree := ts.tree

	tree.Set(testKey(30), ts.shard, ts.position)
	if !tree.testIsValid(t) {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}
	tree.testRequireKey(t, 30)

	tree.Set(testKey(20), ts.shard, ts.position)
	if !tree.testIsValid(t) {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}
	tree.testRequireKey(t, 20)

	tree.Set(testKey(10), ts.shard, ts.position)
	tree.testRequireKey(t, 10)

	tree.printTree("-----------------")
	if !tree.testIsValid(t) {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}
}

func TestAvlInsertLR(t *testing.T) {
	ts := testInitialize(t, true)
	tree := ts.tree

	tree.Set(testKey(30), ts.shard, ts.position)
	if !tree.testIsValid(t) {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	tree.Set(testKey(10), ts.shard, ts.position)
	if !tree.testIsValid(t) {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	tree.Set(testKey(20), ts.shard, ts.position)
	tree.printTree("-----------------")
	if !tree.testIsValid(t) {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}
}

func TestAvlInsertRL(t *testing.T) {
	ts := testInitialize(t, true)
	tree := ts.tree

	tree.Set(testKey(10), ts.shard, ts.position)
	if !tree.testIsValid(t) {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	tree.Set(testKey(30), ts.shard, ts.position)
	if !tree.testIsValid(t) {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	tree.Set(testKey(20), ts.shard, ts.position)
	tree.printTree("-----------------")
	if !tree.testIsValid(t) {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}
}

func TestAvlInsertRR(t *testing.T) {
	ts := testInitialize(t, true)
	tree := ts.tree

	tree.Set(testKey(10), ts.shard, ts.position)
	if !tree.testIsValid(t) {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	tree.Set(testKey(20), ts.shard, ts.position)
	if !tree.testIsValid(t) {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	tree.Set(testKey(30), ts.shard, ts.position)
	tree.printTree("-----------------")
	if !tree.testIsValid(t) {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}
}

func TestAvlMultiLevel(t *testing.T) {
	ts := testInitialize(t, true)
	tree := ts.tree

	tree.Set(testKey(2461), ts.shard, ts.position)
	tree.printTree("-----------------")
	tree.Set(testKey(1902), ts.shard, ts.position)
	tree.printTree("-----------------")
	tree.Set(testKey(2657), ts.shard, ts.position)
	tree.printTree("-----------------")
	tree.Set(testKey(7812), ts.shard, ts.position)
	tree.printTree("-----------------")
	tree.Set(testKey(4865), ts.shard, ts.position)
	tree.printTree("-----------------")
	tree.Set(testKey(7999), ts.shard, ts.position)

	if !tree.testIsValid(t) {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}
}

func TestAvlMultiLevel2(t *testing.T) {
	ts := testInitialize(t, true)
	tree := ts.tree

	tree.Set(testKey(686), ts.shard, ts.position)
	tree.printTree("-----------------")
	tree.Set(testKey(959), ts.shard, ts.position)
	tree.printTree("-----------------")
	tree.Set(testKey(1522), ts.shard, ts.position)
	tree.printTree("-----------------")
	tree.Set(testKey(7275), ts.shard, ts.position)
	tree.printTree("-----------------")
	tree.Set(testKey(7537), ts.shard, ts.position)
	tree.printTree("-----------------")
	tree.Set(testKey(5749), ts.shard, ts.position)

	if !tree.testIsValid(t) {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}
}

func TestAvlMultiLevel3(t *testing.T) {
	ts := testInitialize(t, true)
	tree := ts.tree

	tree.printTree("------------")
	tree.Set(testKey(7150), ts.shard, ts.position)
	tree.printTree("------------")
	tree.Set(testKey(6606), ts.shard, ts.position)
	tree.printTree("------------")
	tree.Set(testKey(2879), ts.shard, ts.position)
	tree.printTree("------------")
	tree.Set(testKey(6229), ts.shard, ts.position)
	tree.printTree("------------")
	tree.Set(testKey(5222), ts.shard, ts.position)
	tree.printTree("------------")
	tree.Set(testKey(7150), ts.shard, ts.position)

	if !tree.testIsValid(t) {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}
}

func TestAvlMultiLevel4(t *testing.T) {
	ts := testInitialize(t, true)
	tree := ts.tree

	tree.printTree("------------")
	tree.Set(testKey(5499), ts.shard, ts.position)
	tree.printTree("------------")
	tree.Set(testKey(7982), ts.shard, ts.position)
	tree.printTree("------------")
	tree.Set(testKey(7434), ts.shard, ts.position)
	tree.printTree("------------")
	tree.Set(testKey(2050), ts.shard, ts.position)
	tree.printTree("------------")
	tree.Set(testKey(2142), ts.shard, ts.position)
	tree.printTree("------------")
	tree.Set(testKey(6523), ts.shard, ts.position)

	if !tree.testIsValid(t) {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}
}

func TestAvlMultiLevel5(t *testing.T) {
	ts := testInitialize(t, true)
	tree := ts.tree

	tree.printTree("------------")
	tree.Set(testKey(2249), ts.shard, ts.position)
	tree.printTree("------------")
	tree.Set(testKey(5158), ts.shard, ts.position)
	tree.printTree("------------")
	tree.Set(testKey(6160), ts.shard, ts.position)
	tree.printTree("------------")
	tree.Set(testKey(4987), ts.shard, ts.position)
	tree.printTree("------------")
	tree.Set(testKey(896), ts.shard, ts.position)
	tree.printTree("------------")
	tree.Set(testKey(658), ts.shard, ts.position)
	tree.printTree("------------")
	tree.Set(testKey(7425), ts.shard, ts.position)
	tree.printTree("------------")
	tree.Set(testKey(7866), ts.shard, ts.position)

	if !tree.testIsValid(t) {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}
}

func TestAvlDeleteRoot(t *testing.T) {
	ts := testInitialize(t, true)
	tree := ts.tree

	tree.Set(testKey(30), ts.shard, ts.position)
	if !tree.testIsValid(t) {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	tree.Delete(testKey(30))
	if !tree.testIsValid(t) {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	if tree.testRootNode(t) != nil {
		t.Fatal("not deleted")
	}
}

func TestAvlDeleteLeft(t *testing.T) {
	ts := testInitialize(t, true)
	tree := ts.tree

	tree.Set(testKey(30), ts.shard, ts.position)
	tree.Set(testKey(20), ts.shard, ts.position)
	if !tree.testIsValid(t) {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	tree.Delete(testKey(20))
	if !tree.testIsValid(t) {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	if tree.testCountEach(t) != 1 {
		t.Fatal("not deleted")
	}

	tree.testRequireKey(t, 30)
}

func TestAvlDeleteRight(t *testing.T) {
	ts := testInitialize(t, true)
	tree := ts.tree

	tree.Set(testKey(30), ts.shard, ts.position)
	tree.Set(testKey(40), ts.shard, ts.position)
	if !tree.testIsValid(t) {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	tree.Delete(testKey(40))
	if !tree.testIsValid(t) {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	if tree.testCountEach(t) != 1 {
		t.Fatal("not deleted")
	}

	tree.testRequireKey(t, 30)
}

func TestAvlDeletePromoteLeft(t *testing.T) {
	ts := testInitialize(t, true)
	tree := ts.tree

	tree.Set(testKey(30), ts.shard, ts.position)
	tree.Set(testKey(20), ts.shard, ts.position)
	if !tree.testIsValid(t) {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	tree.Delete(testKey(30))
	if !tree.testIsValid(t) {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	if tree.testCountEach(t) != 1 {
		t.Fatal("not deleted")
	}

	if testGetRoot(tree) != 20 {
		t.Fatal("unexpected root key")
	}

	tree.testRequireKey(t, 20)
}

func TestAvlDeletePromoteRight(t *testing.T) {
	ts := testInitialize(t, true)
	tree := ts.tree

	tree.Set(testKey(30), ts.shard, ts.position)
	tree.Set(testKey(40), ts.shard, ts.position)
	if !tree.testIsValid(t) {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	tree.Delete(testKey(30))
	if !tree.testIsValid(t) {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	if tree.testCountEach(t) != 1 {
		t.Fatal("not deleted")
	}

	if testGetRoot(tree) != 40 {
		t.Fatal("unexpected root key")
	}

	tree.testRequireKey(t, 40)
}

func TestAvlDeletePromoteLeftFull(t *testing.T) {
	ts := testInitialize(t, true)
	tree := ts.tree

	tree.Set(testKey(30), ts.shard, ts.position)
	tree.Set(testKey(20), ts.shard, ts.position)
	tree.Set(testKey(40), ts.shard, ts.position)
	if !tree.testIsValid(t) {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	tree.Delete(testKey(30))
	if !tree.testIsValid(t) {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	if tree.testCountEach(t) != 2 {
		t.Fatal("not deleted")
	}

	if testGetRoot(tree) != 20 {
		t.Fatal("unexpected root key")
	}

	tree.testRequireKey(t, 20)
	tree.testRequireKey(t, 40)
	tree.testRequireNoKey(t, 30)
}

func TestAvlDeleteReplace(t *testing.T) {
	ts := testInitialize(t, true)
	tree := ts.tree

	tree.Set(testKey(30), ts.shard, ts.position)
	tree.Set(testKey(20), ts.shard, ts.position)
	tree.Set(testKey(40), ts.shard, ts.position)
	tree.Set(testKey(25), ts.shard, ts.position)
	tree.Set(testKey(15), ts.shard, ts.position)
	if !tree.testIsValid(t) {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	if testGetRoot(tree) != 30 {
		t.Fatal("root key not 30")
	}

	tree.Delete(testKey(20))
	if !tree.testIsValid(t) {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	if tree.testCountEach(t) != 4 {
		t.Fatal("not deleted")
	}

	if testGetRoot(tree) != 30 {
		t.Fatal("root key not 30 anymore")
	}

	tree.testRequireKey(t, 25)
	tree.testRequireKey(t, 40)
	tree.testRequireNoKey(t, 20)
}

func TestAvlDeleteReplace2(t *testing.T) {
	ts := testInitialize(t, true)
	tree := ts.tree

	tree.Set(testKey(30), ts.shard, ts.position)
	tree.Set(testKey(20), ts.shard, ts.position)
	tree.Set(testKey(40), ts.shard, ts.position)
	tree.Set(testKey(25), ts.shard, ts.position)
	tree.Set(testKey(15), ts.shard, ts.position)
	tree.Set(testKey(35), ts.shard, ts.position)
	tree.Set(testKey(45), ts.shard, ts.position)
	tree.Set(testKey(17), ts.shard, ts.position)
	if !tree.testIsValid(t) {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	if testGetRoot(tree) != 30 {
		t.Fatal("root key not 30")
	}

	tree.Delete(testKey(20))
	if !tree.testIsValid(t) {
		tree.printTree("-----------------")
		t.Fatal("imbalanced")
	}

	if tree.testCountEach(t) != 7 {
		t.Fatal("not deleted")
	}

	if testGetRoot(tree) != 30 {
		t.Fatal("root key not 30 anymore")
	}

	tree.testRequireKey(t, 17)
	tree.testRequireKey(t, 40)
	tree.testRequireNoKey(t, 20)
}

func TestAvlInsertDelete5(t *testing.T) {
	ts := testInitialize(t, true)
	tree := ts.tree

	tree.printTree("------------")
	tree.Set(testKey(2460), ts.shard, ts.position)
	tree.printTree("------------")
	tree.Set(testKey(7435), ts.shard, ts.position)
	tree.printTree("------------")
	tree.Set(testKey(2460), ts.shard, ts.position)

	if !tree.testIsValid(t) {
		t.Fatal("imbalanced")
	}

	tree.printTree("------------")
	tree.Delete(testKey(-2460))

	if !tree.testIsValid(t) {
		t.Fatal("imbalanced")
	}

	tree.printTree("------------")
	tree.Delete(testKey(2460))

	if !tree.testIsValid(t) {
		t.Fatal("imbalanced")
	}
}

func TestAvlInsertDelete6(t *testing.T) {
	ts := testInitialize(t, true)
	tree := ts.tree

	tree.printTree("------------")
	tree.Set(testKey(7472), ts.shard, ts.position)
	tree.printTree("------------")
	tree.Set(testKey(2576), ts.shard, ts.position)
	tree.printTree("------------")
	tree.Set(testKey(2813), ts.shard, ts.position)
	tree.printTree("------------")
	tree.Set(testKey(5622), ts.shard, ts.position)
	tree.printTree("------------")
	tree.Set(testKey(7109), ts.shard, ts.position)
	tree.printTree("------------")
	tree.Delete(testKey(2576))
	tree.printTree("------------")

	if !tree.testIsValid(t) {
		t.Fatal("imbalanced")
	}
}

func TestAvlInsertDelete22(t *testing.T) {
	ts := testInitialize(t, true)
	tree := ts.tree

	tree.printTree("------------")
	tree.Set(testKey(743), ts.shard, ts.position)
	tree.printTree("------------")
	tree.Set(testKey(6999), ts.shard, ts.position)
	tree.printTree("------------")
	tree.Set(testKey(7700), ts.shard, ts.position)
	tree.printTree("------------")
	tree.Set(testKey(5829), ts.shard, ts.position)
	tree.printTree("------------")
	tree.Set(testKey(5898), ts.shard, ts.position)
	tree.printTree("------------")
	tree.Set(testKey(7508), ts.shard, ts.position)
	tree.printTree("------------")
	tree.Delete(testKey(5898))
	tree.printTree("------------")
	tree.Delete(testKey(6999))
	tree.printTree("------------")
	tree.Set(testKey(5096), ts.shard, ts.position)
	tree.printTree("------------")
	tree.Set(testKey(5766), ts.shard, ts.position)
	tree.printTree("------------")
	tree.Set(testKey(7801), ts.shard, ts.position)
	tree.printTree("------------")
	tree.Set(testKey(5557), ts.shard, ts.position)
	tree.printTree("------------")
	tree.Set(testKey(6492), ts.shard, ts.position)
	tree.printTree("------------")
	tree.Delete(testKey(5766))
	tree.printTree("------------")
	tree.Delete(testKey(743))
	tree.printTree("------------")
	tree.Set(testKey(4230), ts.shard, ts.position)
	tree.printTree("------------")
	tree.Set(testKey(2066), ts.shard, ts.position)
	tree.printTree("------------")
	tree.Set(testKey(1668), ts.shard, ts.position)
	tree.printTree("------------")
	tree.Delete(testKey(5829))
	tree.printTree("------------")
	tree.Set(testKey(3929), ts.shard, ts.position)
	tree.printTree("------------")
	tree.Set(testKey(2455), ts.shard, ts.position)
	tree.printTree("------------")
	tree.Set(testKey(2580), ts.shard, ts.position)
	tree.printTree("------------")

	if !tree.testIsValid(t) {
		t.Fatal("imbalanced")
	}

}

func TestAvlDeleteLinks(t *testing.T) {
	ts := testInitialize(t, true)
	tree := ts.tree

	tree.printTreePositions("------------")
	tree.Set(testKey(6122), 14394257565589590259, 1)
	tree.printTreePositions("------------")
	tree.Set(testKey(1454), 14394257565589590259, 2)
	tree.printTreePositions("------------")
	tree.Set(testKey(2202), 14394257565589590259, 3)
	tree.printTreePositions("------------")
	tree.Set(testKey(7180), 14394257565589590259, 4)
	tree.printTreePositions("------------")
	tree.Set(testKey(8163), 14394257565589590259, 5)
	tree.printTreePositions("------------")
	tree.Delete(testKey(7180))
}

func testInsertDelete(t *testing.T, worst int) (out []int) {
	ts := testInitialize(t, true)
	tree := ts.tree

	history := make([]int, 0, 1024)
	historyPtr := &history

	defer func() {
		if r := recover(); r != nil {
			out = *historyPtr
		}
	}()

	ops := 0

	numbers := make([]int, 0, 1024)
	table := map[int]struct{}{}

	for i := 0; i < (3 * 1024); i++ {
		if i%3 > 0 {
			// Find
			if len(numbers) > 0 {
				target := mrand.Intn(len(numbers))
				targetNumber := numbers[target]
				_, isset := table[targetNumber]
				if i%3 == 1 {
					n, err := tree.Find(testKeyInt(targetNumber))
					if err != nil {
						t.Fatal(err)
					}
					if n == nil {
						if isset {
							t.Fatalf("expected to find %d", targetNumber)
						}
					}
				} else {
					n, err := tree.Find(testKeyInt(-targetNumber))
					if err != nil {
						t.Fatal(err)
					}
					if n != nil {
						if !isset {
							t.Fatalf("didn't expect to find %d", targetNumber)
						}
					}
				}
			}
			continue
		}

		op := mrand.Intn(4)
		var v int
		if op == 0 && len(numbers) > 0 {
			n := mrand.Intn(len(numbers))
			v = -numbers[n]
			numbers = append(numbers[0:n], numbers[n+1:]...)
		} else {
			v = mrand.Intn(8192) + 1
		}

		ops++
		*historyPtr = append(*historyPtr, v)
		if v > 0 {
			numbers = append(numbers, v)
			tree.Set(testKeyInt(v), 0, 0)
			table[v] = struct{}{}
		} else {
			tree.Delete(testKeyInt(-v))
			delete(table, -v)
		}
		if !tree.testIsValid(t) {
			if worst == 0 || len(*historyPtr) < worst {
				out = *historyPtr
			}
			break
		}

		for v := range table {
			node, err := tree.Find(testKeyInt(v))
			if err != nil {
				t.Fatal(err)
			}
			if node == nil {
				if v >= 0 {
					t.Fatalf("didn't find %v", v)
				}
			} else {
				if v < 0 {
					t.Fatalf("shouldn't have found %v", v)
				}
			}
		}
	}

	fmt.Printf("%d operations, %d values in the tree: PASS\n", ops, len(table))
	return
}

func TestAvlInsertDeleteRandom(t *testing.T) {
	ts := testInitialize(t, true)
	tree := ts.tree

	var worst []int
	for pass := 0; pass < 100; pass++ {
		worst = testInsertDelete(t, len(worst))
		if len(worst) > 0 {
			break
		}
	}

	if worst != nil {
		for n, v := range worst {
			fmt.Println("tree.printTree(\"------------\")")
			if v > 0 {
				fmt.Printf("tree.Set(testKey(%v),%d,%d)\n", v, ts.shard, n+1)
			} else {
				fmt.Printf("tree.Delete(testKey(%v))\n", -v)
			}
			tree.Set(testKeyInt(v), ts.shard, ts.position)
		}
		tree.printTree("------imbalanced------")
		fmt.Printf("%d steps\n", len(worst))
		t.Fatal("imbalanced")
	}
}
