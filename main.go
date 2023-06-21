

package main
import(
	"github.com/syndtr/goleveldb/leveldb"
	"log"
	"fmt"
	"encoding/json"
	"encoding/hex"
	"crypto/sha256"
	"sync"
	"os"
	//"bufio"
	"time"
	"io/ioutil"
	"strings"
	
) 
type transaction struct{
	Key string              `json:"key"`
	Data struct {
		Val int               `json:"val"`
		Ver  float64           `json:"ver"`
	}`json:"data"`
	Valid bool               `json:"valid"`
	TransactionHash string    `json:"transactionHash"`
}

type BlockStatus string
const(
	Commited BlockStatus ="commited"
	Pending BlockStatus ="pending"
)


type Block struct{
	BlockNumber int   `json:"blockNumber"`
	Txns []transaction   `json:"transaction"`
	TimeStamp time.Duration         `json:"blockProcessingTime"`
	Status BlockStatus  `json:"status"`
	PrevBlockHash string      `json:"prevBlockHash"`
	BlockHash string           `json:"blockHash"`
}

type blockInterface interface{
	push(Txns []transaction,db *leveldb.DB)
	update(Status BlockStatus)
}



func main(){

//creating 1000 entries in leveldb
	db,err:=leveldb.OpenFile("data/levelDb",nil)
	if err != nil{
	log.Fatal(err)
 }
defer db.Close()

for i:=1;i<=1000;i++ {
	txn := transaction {
		Key : fmt.Sprintf("SIM%d",i),
		Data:struct{
			Val int               `json:"val"`
		Ver  float64           `json:"ver"`
		}{
			Val:i,
			Ver:1.0,
		},
	}
	data,err:=json.Marshal(txn)
	if err != nil {
		log.Println("error encoding transaction",err)
		continue
	}
	err=db.Put([]byte(txn.Key),data,nil)
	if err != nil{
	log.Println("Error storing the transaction",err)
    }


 }

 //reading input data from input.json file

fileForInput := "input.json"
inputJson,err:=ioutil.ReadFile(fileForInput)
if err != nil {
	log.Fatal("Error while reading file:",err)
}

var Txns []transaction
err=json.Unmarshal([]byte(inputJson),&Txns)
if err != nil {
	log.Fatal("error sorry",err)
}

//calculating transaction hashes concurently
wg :=&sync.WaitGroup{}
wg.Add(len(Txns))

for i:=0;i<len(Txns);i++ {
	go calculateTransactionHash(&Txns[i],wg)
}

wg.Wait()

filePath:="blocks"
blockSize:=2
myChan:=make(chan Block)
go addTransactionToBlock(Txns,db,blockSize,myChan)
blocks:=[]Block{}
index:=0
for block:=range myChan{
	if block.BlockNumber == 1{
		block.PrevBlockHash="0xabc123"
		block.BlockHash=calculateBlockHash(block)

	}else{
		block.PrevBlockHash=blocks[index-1].BlockHash
		block.BlockHash=calculateBlockHash(block)

	}
	blocks=append(blocks,block)
	index++
	
	
	addBlockToFile(filePath,block)


}







for {
	fmt.Println("Enter a number of your choice :")
	fmt.Println("1.To get Block by number\n2.To fetch details of all blocks\n3.display processing time for each block\n4.To exit")
	var number int 
	_,err:=fmt.Scanln(&number)
	if err != nil {
		fmt.Println("Invalid input")
		continue
	}

	switch number{
	case 1:
        fmt.Println("Enter the block Number you wanted to see")
		var input1 int
		_,err:=fmt.Scanln(&input1)
		if err != nil {
			fmt.Println("Invalid input")
			continue
		}
		findByBlockNumber(input1)
	case 2:
		GetAllBlocksFromFile(filePath)
	case 3:
		displayBlockProcessing(filePath)
        
	case 4:
		fmt.Println("Exiting from the program")
		return

    default:
        fmt.Println("Number is not 1, 2, or 3.")
   }
}



}//main end


func addTransactionToBlock(Txns []transaction,db *leveldb.DB,blockSize int,myChan chan<-Block){
	totalTransactions:=len(Txns)
	numBlocks:=totalTransactions/blockSize
	if(totalTransactions % numBlocks !=0){
		numBlocks++
		fmt.Println(numBlocks)
	}
	for i:=0;i<numBlocks;i++{
		start:=i*blockSize
		end:=(i+1)*blockSize
		if end > totalTransactions {
			end=totalTransactions
		}
		blockAStartTime := time.Now()
		block:=Block{
			BlockNumber:i+1,
			TimeStamp:12,
			Status:Pending,
		}
		
		block.push(Txns[start:end],db)
		
		block.update(Commited)
		blockADuration := time.Since(blockAStartTime)
		block.TimeStamp=blockADuration
		myChan <- block
	}
	close(myChan)
}
func (block *Block)update(Status BlockStatus){
	block.Status=Status
}

func (block *Block) push(Txns []transaction,db *leveldb.DB){
	for i:=0;i<len(Txns);i++{
		
		res:=validate(Txns[i],db)
		if(res){
			txn := transaction{
				Key:Txns[i].Key,
				Data:struct{
					Val int               `json:"val"`
	            	Ver  float64           `json:"ver"`
				}{
					Val:Txns[i].Data.Val,
					Ver:Txns[i].Data.Ver+1,

				},
				TransactionHash:Txns[i].TransactionHash,
			}
			txn.Valid=true
			txn.TransactionHash=Txns[i].TransactionHash
			block.Txns=append(block.Txns,txn)
			data,err:=json.Marshal(txn)
			if err != nil{
				log.Println("Error while encoding",err)
				continue
			}
			err=db.Put([]byte(txn.Key),data,nil)
			if err != nil{
				log.Println("Error while storing transaction in db",err)
			}
		}else{
			

			data, err := db.Get([]byte(Txns[i].Key), nil)
			if err != nil {
				
					return
				}
				var temp transaction
				err=json.Unmarshal(data,&temp)
				if err != nil {
					return 
				}
				temp.Valid=res
				temp.TransactionHash=Txns[i].TransactionHash
				block.Txns=append(block.Txns,temp)
			
			
			
		}
	}
	
}

func validate(txn transaction,db *leveldb.DB) bool{
	value,err := db.Get([]byte(txn.Key),nil)
	if(err != nil){
		return false
	}
	var data transaction
	err=json.Unmarshal(value,&data)
	if err != nil{
		return false
	}
	if data.Data.Ver == txn.Data.Ver {
		return true
	}
	return false
}

func calculateTransactionHash(txn *transaction,wg *sync.WaitGroup){
	
	defer wg.Done()
	temp:=fmt.Sprintf("%v",txn)
	txnBytes:=[]byte(temp)
	hash:=sha256.Sum256(txnBytes)
	txnHash:=hex.EncodeToString(hash[:])
	//return txnHash
	txn.TransactionHash=txnHash
	
	

}
func addBlockToFile(filePath string,block Block) {

				if blockNumberExists(block.BlockNumber) {
					fmt.Println("Block already exists in the file")
					return 
				}

				data, err := json.Marshal(block)
				if err != nil {
					fmt.Println("Error marshalling Block:", err)
					return 
				}
				file, err := os.OpenFile("block.json", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					fmt.Println("Error opening file:", err)
					return 
				}
				defer file.Close()
			
				// Append the JSON data to the file
				_, err = file.WriteString(string(data) + "\n")
				if err != nil {
					fmt.Println("Error writing to file:", err)
					return 
				}
				return 



	
}
func GetAllBlocksFromFile(filename string) {

file, err := os.Open("block.json")
if err != nil {
    fmt.Println("Error opening file:", err)
    return 
}
defer file.Close()
contents, err := ioutil.ReadAll(file)
if err != nil {
    fmt.Println("Error reading file:", err)
    return 
}
fmt.Println("The blocks are listed below:")

for _, line := range strings.Split(string(contents), "\n") {
    if line == "" {
        continue 
    }

    block := Block{}
    err := json.Unmarshal([]byte(line), &block)
    if err != nil {
        fmt.Println("Error unmarshalling block:", err)
        return 
    }
	fmt.Printf("BLOCK NUMBER %d: \n\n",block.BlockNumber)
	fmt.Println(block)
	fmt.Println()

  }
  fmt.Println("*********************************")

}



func findByBlockNumber(blockNumber int){
	file, err := os.Open("block.json")
    if err != nil {
    fmt.Println("Error opening file:", err)
    return
    }
   defer file.Close()
     contents, err := ioutil.ReadAll(file)
     if err != nil {
    fmt.Println("Error reading file:", err)
    return
    }
   blocks := []Block{}
   for _, line := range strings.Split(string(contents), "\n") {
     if line == "" {
        continue 
    }

    block := Block{}
    err := json.Unmarshal([]byte(line), &block)
    if err != nil {
        fmt.Println("Error unmarshalling block:", err)
        return
    }

    blocks = append(blocks, block)
   }
    desiredBlockNumber := blockNumber 

     var desiredBlock Block
   for _, block := range blocks {
    if block.BlockNumber == desiredBlockNumber {
        desiredBlock = block
		fmt.Println("found")
        break
     }
   }

   if desiredBlock.BlockNumber == 0 {
    fmt.Println("Block not found")
    return
   }

fmt.Printf("Block Number:%d\n",blockNumber)
fmt.Println(desiredBlock)
fmt.Println("*********************************")



}
func blockNumberExists(blockNumber int) bool{
	file, err := os.Open("block.json")
if err != nil {
    fmt.Println("Error opening file:", err)
    return false
}
defer file.Close()
contents, err := ioutil.ReadAll(file)
if err != nil {
    fmt.Println("Error reading file:", err)
    return false
}
blocks := []Block{}
for _, line := range strings.Split(string(contents), "\n") {
    if line == "" {
        continue 
    }

    block := Block{}
    err := json.Unmarshal([]byte(line), &block)
    if err != nil {
        fmt.Println("Error unmarshalling block:", err)
        return false
    }

    blocks = append(blocks, block)
}
desiredBlockNumber := blockNumber 


for _, block := range blocks {
    if block.BlockNumber == desiredBlockNumber {
        
        return true
    }
}

return false



}

func calculateBlockHash(block Block) string {
	blockBytes, _ := json.Marshal(block)
	hashBytes := sha256.Sum256(blockBytes)
	return fmt.Sprintf("%x", hashBytes)
}



func displayBlockProcessing(filePath string){
file, err := os.Open("block.json")
if err != nil {
    fmt.Println("Error opening file:", err)
    return 
}
defer file.Close()
contents, err := ioutil.ReadAll(file)
if err != nil {
    fmt.Println("Error reading file:", err)
    return 
}
fmt.Println("The processing time for ech block is listed below:")

for _, line := range strings.Split(string(contents), "\n") {
    if line == "" {
        continue 
    }

    block := Block{}
    err := json.Unmarshal([]byte(line), &block)
    if err != nil {
        fmt.Println("Error unmarshalling block:", err)
        return 
    }
	fmt.Printf("The time required for the block number %d is: ",block.BlockNumber)
	fmt.Println(block.TimeStamp)

  }
  fmt.Println("*********************************")
}

