package mbtiles

import (
	"database/sql"
	"github.com/hokkaido/blink"
	"log"
	"net/url"
	"os"
	_ "github.com/hokkaido/blink-mbtiles/third_party/github.com/mattn/go-sqlite3"
)

func init() {
	blink.RegisterProvider("mbtiles", NewProvider, NewConfig)
}

type MBTile struct {
	batchSize int
	db        *sql.DB
	open      bool
}

type Config struct {
	PathToDb  string
	BatchSize int
}

func NewConfig() interface{} {
	return &Config{BatchSize: 100}
}

func NewProvider(providerConfig interface{}) (blink.Provider, error) {

	config, ok := providerConfig.(*Config)
	if !ok {
		panic("wrong config")
	}
	log.Print("OMG")
	log.Print(config.PathToDb)
	_, err := os.Stat(config.PathToDb)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite3", config.PathToDb)

	if err != nil {
		return nil, err
	}

	return &MBTile{db: db, batchSize: config.BatchSize, open: true}, nil
}

func (mbtile *MBTile) GetTile(zoom int, x int, y int) ([]byte, error) {

	var sql = "SELECT tile_data FROM tiles WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?"

	// Flip y-Axis
	y = (1 << uint(zoom)) - 1 - y

	stmt, err := mbtile.db.Prepare(sql)
	if err != nil {
		return nil, err
	}

	defer stmt.Close()

	var tileData []byte

	err = stmt.QueryRow(zoom, x, y).Scan(&tileData)
	if err != nil {
		return nil, err
	}

	return tileData, nil
}

// Select a grid and its data from an mbtiles database
func (mbtile *MBTile) GetGrid(zoom int, x int, y int) ([]byte, error) {

	var sqlgrid = "SELECT grid FROM grids WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?"
	//var sqljson = "SELECT key_name, key_json FROM grid_data WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?"

	stmt, err := mbtile.db.Prepare(sqlgrid)
	if err != nil {
		return nil, err
	}

	defer stmt.Close()

	var gridData []byte

	err = stmt.QueryRow(zoom, x, y).Scan(&gridData)
	if err != nil {
		return nil, err
	}

	return gridData, nil
}

func (mbtile *MBTile) GetMetadata(u *url.URL) {

	rows, err := mbtile.db.Query("Select name, value FROM metadata")
	if err != nil {
		return
	}

	//values := make(map[string]string)

	defer rows.Close()

	for rows.Next() {
		var name, value string
		rows.Scan(&name, &value)
		switch name {
		case "json":

		case "":

		case "tatat":
		}

	}

}

// Closes the underlying Sqlite Database
func (mbtile *MBTile) Close() {
	mbtile.db.Close()
}

type Metadata struct {
	scheme   string
	basename string
	id       string
	minZoom  int
	maxZoom  int
	Values   map[string]string
}

func integrityCheck() {

}
