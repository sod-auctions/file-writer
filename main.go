package file_writer

import (
	"log"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

type FileWriter struct {
	FileName string
	lw       source.ParquetFile
	pw       *writer.ParquetWriter
}

type Record struct {
	RealmID        int32 `parquet:"name=realmId, type=INT32"`
	AuctionHouseID int32 `parquet:"name=auctionHouseId, type=INT32"`
	ItemID         int32 `parquet:"name=itemId, type=INT32"`
	Bid            int32 `parquet:"name=bid, type=INT32"`
	Buyout         int32 `parquet:"name=buyout, type=INT32"`
	BuyoutEach     int32 `parquet:"name=buyoutEach, type=INT32"`
	Quantity       int32 `parquet:"name=quantity, type=INT32"`
	TimeLeft       int32 `parquet:"name=timeLeft, type=INT32"`
}

func NewFileWriter(fileName string) *FileWriter {
	lw, err := local.NewLocalFileWriter(fileName)
	if err != nil {
		log.Fatal("Can't create local file", err)
	}

	pw, err := writer.NewParquetWriter(lw, new(Record), 4)
	if err != nil {
		log.Fatal("Can't create parquet writer", err)
	}

	pw.CompressionType = parquet.CompressionCodec_UNCOMPRESSED

	return &FileWriter{
		FileName: fileName,
		lw:       lw,
		pw:       pw,
	}
}

func (fw *FileWriter) Write(record *Record) error {
	err := fw.pw.Write(record)
	if err != nil {
		return err
	}
	return nil
}

func (fw *FileWriter) Close() error {
	err := fw.pw.WriteStop()
	if err != nil {
		return err
	}

	err = fw.lw.Close()
	if err != nil {
		return err
	}

	return nil
}
