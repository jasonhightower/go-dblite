package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
    STATEMENT_INSERT = 1
    STATEMENT_SELECT = 2
    COLUMN_USERNAME_SIZE = 32
    COLUMN_EMAIL_SIZE = 255

    ID_SIZE = 4
    ROW_SIZE = ID_SIZE + COLUMN_USERNAME_SIZE + COLUMN_EMAIL_SIZE
    PAGE_SIZE = 4096
    TABLE_MAX_PAGES = 100
    ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE
    TABLE_MAX_ROWS = TABLE_MAX_PAGES * ROWS_PER_PAGE
)

func resizeString(str string, size int) string {
    if len(str) == size {
        return str
    }
    if len(str) > size {
        return str[:size]
    }
    return fmt.Sprintf("%*s", size - len(str), str)
}

type Row struct {
    Id uint32
    Username [COLUMN_USERNAME_SIZE]byte
    Email [COLUMN_EMAIL_SIZE]byte
}
func (r *Row) String() string {
    return fmt.Sprintf(" %d | %s | %s \n", r.Id, r.Username, r.Email)  
}

func NewRow(id uint32, username string, email string) (*Row) {
    row := Row{
        Id: id,
    }
    copy(row.Username[:], username)
    copy(row.Email[:], email)
    return &row
}
type Page struct {
    Rows [PAGE_SIZE]byte
}
type Table struct {
    RowCount uint32
    Pages [TABLE_MAX_PAGES]*[PAGE_SIZE]byte // array of pointers to byte arrays for each page
}

func (t *Table) rowLocation(id uint32, alloc bool) (*[PAGE_SIZE]byte, uint, error) {
    pageIndex := id / ROWS_PER_PAGE
    page := t.Pages[pageIndex]
    if page == nil {
        if alloc {
            var newPage [PAGE_SIZE]byte
            t.Pages[pageIndex] = &newPage
            page = t.Pages[pageIndex]
        } else {
            return nil, 0, fmt.Errorf("Id %d not found", id)
        }
    }
    rowInPage := id % ROWS_PER_PAGE
    pageOffset := rowInPage * ROW_SIZE
    return page, uint(pageOffset), nil
}
func (t *Table) Insert(row *Row) (error) {
    if t.RowCount == TABLE_MAX_ROWS {
        return fmt.Errorf("Table full")
    }
    row.Id = t.RowCount
    if page, offset, err := t.rowLocation(t.RowCount, true); err == nil {
        fmt.Printf("Writing into %d of a %d sized array - %s\n", offset, len(*page), page)
        binary.BigEndian.PutUint32((*page)[offset:], row.Id)
        copy(page[offset + ID_SIZE:], row.Email[:])
        copy(page[offset + ID_SIZE + COLUMN_EMAIL_SIZE:], row.Username[:])
        t.RowCount += 1
        return nil
    } else {
        // TODO JH add a better error message
        return err
    }
}
func (t *Table) Read(rowNum uint32) (*Row, error) {
    // TODO JH sanity check row index
    if page, offset, err := t.rowLocation(uint32(rowNum), false); err != nil {
        return nil, err
    } else {
        row := &Row{}
        row.Id = binary.BigEndian.Uint32(page[offset:])
        copy(row.Email[:], page[offset + ID_SIZE:])
        copy(row.Username[:], page[offset + ID_SIZE + COLUMN_EMAIL_SIZE:])
        return row, nil
    }
}

type Statement struct {
    StatementType int
    InsertRow *Row
}
func NewStatement(statementType int, row *Row) (*Statement) {
    return &Statement{
        StatementType: statementType,
        InsertRow: row,
    }
}


func show_prompt() {
   fmt.Print("dblite > ") 
}

func read_input(reader *bufio.Reader) string {
    text, _ := reader.ReadString('\n') 
    return strings.TrimSpace(text)
}

func executeMetaCommand(instruction string) (error) {
    if instruction == ".exit" {
        os.Exit(0)
    }
    return fmt.Errorf("Unrecognized command '%s'", instruction)
}

func parseStatement(instruction string) (*Statement, error) {
    if len(instruction) < 6 {
        return nil, fmt.Errorf("Unrecognized statement '%s'", instruction)
    }
    if instruction[:6] == "insert" {
        args := strings.Split(instruction[7:], " ")
        if len(args) != 3 {
            return nil, fmt.Errorf("Insert syntax error: got %d arguments expected 3", len(args))
        }
        id, err := strconv.Atoi(args[0])
        if err != nil {
            return nil, fmt.Errorf("Insert syntax error: %s", err.Error())
        }
        row := NewRow(uint32(id), args[1], args[2])
        return NewStatement(STATEMENT_INSERT, row), nil
    }
    if instruction[:6] == "select" {
        return NewStatement(STATEMENT_SELECT, nil), nil
    }
    return nil, fmt.Errorf("Unrecognized statement '%s'", instruction)
}

func main() {
    table := Table{}
    input_reader := bufio.NewReader(os.Stdin)
    for true {
        show_prompt()
        instruction := read_input(input_reader)
        if instruction[0] == '.' {
            if err := executeMetaCommand(instruction); err != nil {
                fmt.Println(err.Error())
                continue
            }
        } else {
            statement, err := parseStatement(instruction)
            if err != nil {
                fmt.Println(err.Error())
                continue
            }
            switch statement.StatementType {
            case STATEMENT_INSERT: 
                 fmt.Println("Executing insert")
                 table.Insert(statement.InsertRow)                 
            case STATEMENT_SELECT:
                 fmt.Println("Executing select")
                for i := uint32(0); i < table.RowCount; i++ {
                    row, err := table.Read(i)
                    if err != nil {
                        fmt.Printf("Error executing select: %s\n", err.Error())
                        continue
                    }
                    fmt.Println(row)
                }
            default:
            }
        }
    }
}
