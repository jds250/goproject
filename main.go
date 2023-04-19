package main

import (
	"encoding/csv"
	"ethereum_code/trie"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"os"
	"strconv"
)

type achivenode struct {
	GlobalTrie  *trie.Trie
	CacheTrie   *trie.Trie
	CurrentTrie *trie.Trie
	db          *leveldb.DB
}

type consensusnode struct {
	CacheTrie   *trie.Trie
	CurrentTrie *trie.Trie
	db          *leveldb.DB
}

func main() {

	var a achivenode
	var c consensusnode
	var err error

	a.db, _ = leveldb.OpenFile("./database", nil)
	if err != nil {
		fmt.Println("error1")
		return
	}
	defer a.db.Close()
	root1 := "56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421" //64字节->ascii码->字节，emptyroot
	//b1 := []byte(root1)
	//v, _ := hex.Decode(b1, b1) //64字节的十六进制字符串被解码为32字节
	//fmt.Println(Hex2Bytes(root1))
	//HexToHash会将输入的root字符串转变为Hash(32字节类型)，若字符串以0x开头，则将0x去掉，若长度为奇数
	//则首项添0，若长度大于32，则选取后32个字节
	a.GlobalTrie, _ = trie.NewTrie(trie.HexToHash(root1), a.db) //档案节点的全局状态树
	a.CacheTrie, _ = trie.NewTrie(trie.HexToHash(root1), a.db)
	a.CurrentTrie, _ = trie.NewTrie(trie.HexToHash(root1), a.db)

	c.db, err = leveldb.OpenFile("./database_0", nil)
	if err != nil {
		fmt.Println("error2")
		return
	}
	defer c.db.Close()

	c.CacheTrie, _ = trie.NewTrie(trie.HexToHash(root1), c.db)
	c.CurrentTrie, _ = trie.NewTrie(trie.HexToHash(root1), c.db)

	achiveTrieSize, _ := a.db.GetProperty("leveldb.stats")
	fmt.Println("pre档案节点存储：", achiveTrieSize)

	consensusTrieSize, _ := c.db.GetProperty("leveldb.stats")
	fmt.Println("pre共识节点存储", consensusTrieSize)

	f, _ := os.Create("./data.txt")
	f2, _ := os.Create("./data2.txt")
	defer f.Close()
	defer f2.Close()

	//blockHead := make([]trie.Header, 3)
	//
	for i := 1; i < 2; i++ {
		filename := "./tx" + strconv.Itoa(i) + ".csv"
		file, err := os.Open(filename)
		if err != nil {
			fmt.Println(err)
		}
		defer file.Close()

		reader := csv.NewReader(file)
		reader.FieldsPerRecord = -1
		csvdata, err := reader.ReadAll()

		for x, tx := range csvdata {
			a.GlobalTrie.Update([]byte(tx[0]), []byte(tx[1]))
			a.CurrentTrie.Update([]byte(tx[0]), []byte(tx[1]))
			if x%999 == 0 {
				a.GlobalTrie.Commit(nil)
				a.CacheTrie.Commit(nil)
				a.CurrentTrie.Commit(nil)
			}
			if (x+1)%40000 == 0 {

				achiveTrieSize, _ = a.db.GetProperty("leveldb.size")
				f.WriteString(achiveTrieSize)
				a.CacheTrie = a.CurrentTrie
				a.CurrentTrie, _ = trie.NewTrie(trie.HexToHash(root1), a.db)
			}
		}
		//stateroot, _ := a.GlobalTrie.Commit(nil)
		//blockHead[i-1] = trie.Header{StateRoot: stateroot}
	}

	////
	//for i := 1; i < 2; i++ {
	//	filename := "./tx" + strconv.Itoa(i) + ".csv"
	//	file, err := os.Open(filename)
	//	if err != nil {
	//		fmt.Println(err)
	//	}
	//	defer file.Close()
	//
	//	reader := csv.NewReader(file)
	//	reader.FieldsPerRecord = -1
	//	csvdata, err := reader.ReadAll()
	//
	//	for x, tx := range csvdata {
	//		c.CurrentTrie.Update([]byte(tx[0]), []byte(tx[1]))
	//
	//		var cache [][][]byte
	//		cache = append(cache, [][]byte{[]byte(tx[0]), []byte(tx[1])})
	//		if x%999 == 0 {
	//			c.CurrentTrie.Commit(nil)
	//			c.CacheTrie.Commit(nil)
	//		}
	//		if (x+1)%50000 == 0 {
	//			index := (x + 1) / 50000
	//
	//			consensusTrieSize, _ = c.db.GetProperty("leveldb.size")
	//			f2.WriteString(consensusTrieSize)
	//
	//			c.db.Close()
	//
	//			c.db, err = leveldb.OpenFile("database"+strconv.Itoa(index), nil)
	//			if err != nil {
	//				panic(err)
	//			}
	//
	//			c.CurrentTrie, _ = trie.NewTrie(trie.HexToHash(root1), c.db)
	//
	//			c.CacheTrie, _ = trie.NewTrie(trie.HexToHash(root1), c.db)
	//
	//			for _, t := range cache {
	//				c.CacheTrie.Update(t[0], t[1])
	//			}
	//		}
	//	}
	//}
	//achiveTrieSize, _ = db.GetProperty("leveldb.stats")
	//fmt.Println("档案节点存储：", achiveTrieSize)
	//consensusTrieSize, _ = db2.GetProperty("leveldb.stats")
	//fmt.Println("共识节点存储", consensusTrieSize)
	//fmt.Println("区块头：", blockHead)
	//tx := []byte("e2d9b095ea37f61df4ad3b57434c3abbe03d7280146cd0a006e896c2a2a7d45f")
	//fmt.Println(achiveTrie.Get(tx))
	//bag := achiveTrie.Getproof(tx)
	//fmt.Println("一笔交易的证明大小", size.Of(bag), "Bytes")

	//tr.Commit(nil)
	//iter := db.NewIterator(nil, nil)
	//for iter.Next() {
	//	fmt.Printf("key:%x,value:%x\n", iter.Key(), iter.Value())
	//}
	//iter.Release()
}
