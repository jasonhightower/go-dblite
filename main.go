package main

import (
    "io"
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

    META_STATE_EXIT = 1
    META_STATE_ERROR = -1
    META_STATE_SUCCESS = 0

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
func strLen(in []byte) int {
    from, to := 0, len(in)
    for from < to {
        index := (from + to) / 2
        if in[index] == 0 {
            to = index
        } else {
            from = index + 1
        }
    }
    return from
}

func (r *Row) String() string {
    emailLen := strLen(r.Email[:])
    userLen := strLen(r.Username[:])
    // should be a better way to do this
    return fmt.Sprintf(" %d | %s | %s", r.Id, string(r.Username[:userLen]), string(r.Email[:emailLen]))  
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
    if t.RowCount >= uint32(TABLE_MAX_ROWS) {
        return fmt.Errorf("Table full.")
    }
    if page, offset, err := t.rowLocation(t.RowCount, true); err == nil {
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


func show_prompt(out *bufio.Writer) {
   out.WriteString("dblite > ") 
   out.Flush()
}

func read_input(reader *bufio.Reader) string {
    text, _ := reader.ReadString('\n') 
    return strings.TrimSpace(text)
}

func executeMetaCommand(instruction string, out *bufio.Writer) (int, error) {
    if instruction == ".exit" {
        return META_STATE_EXIT, nil
    }
    return META_STATE_ERROR, fmt.Errorf("Unrecognized command '%s'", instruction)
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
        if len(args[1]) > COLUMN_USERNAME_SIZE {
            return nil, fmt.Errorf("username is too long")
        }
        if len(args[2]) > COLUMN_EMAIL_SIZE {
            return nil, fmt.Errorf("email is too long");
        }
        id, err := strconv.Atoi(args[0])
        if err != nil {
            return nil, fmt.Errorf("Insert syntax error: %s", err.Error())
        } else if id < 0 {
            return nil, fmt.Errorf("ID must be positive.")
        }
        row := NewRow(uint32(id), args[1], args[2])
        return NewStatement(STATEMENT_INSERT, row), nil
    }
    if instruction[:6] == "select" {
        return NewStatement(STATEMENT_SELECT, nil), nil
    }
    return nil, fmt.Errorf("Unrecognized statement '%s'", instruction)
}

func exec(in *io.Reader, out *io.Writer) {
    bufIn := bufio.NewReader(*in)
    bufOut := bufio.NewWriter(*out)
    table := Table{}
    for true {
        show_prompt(bufOut)
        instruction := read_input(bufIn)
        if instruction[0] == '.' {
            if state, err := executeMetaCommand(instruction, bufOut); state == META_STATE_ERROR {
                bufOut.WriteString(err.Error())
                bufOut.WriteString("\n")
                continue
            } else if state == META_STATE_EXIT {
                bufOut.WriteString("Exiting.\n")
                bufOut.Flush()
                return
            }
        } else {
            statement, err := parseStatement(instruction)
            if err != nil {
                bufOut.WriteString(err.Error())
                bufOut.WriteString("\n")
                continue
            }
            switch statement.StatementType {
            case STATEMENT_INSERT: 
                 bufOut.WriteString("Executing\n")
                 err := table.Insert(statement.InsertRow)                 
                 if err != nil {
                    bufOut.WriteString(fmt.Sprintf("Error: %s\n", err.Error()))
                    bufOut.Flush()
                    continue
                 }
            case STATEMENT_SELECT:
                 bufOut.WriteString("Executing\n")
                 for i := uint32(0); i < table.RowCount; i++ {
                    row, err := table.Read(i)
                    if err != nil {
                        bufOut.WriteString("Error executing select: ")
                        bufOut.WriteString(err.Error())
                        bufOut.WriteString("\n")
                        continue
                    }
                    bufOut.WriteString(row.String())
                    bufOut.WriteString("\n")
                }
            default:
            }
        }
    }
}

func main() {
    var out io.Writer = os.Stdout
    var in io.Reader = os.Stdin
    exec(&in, &out)
}
