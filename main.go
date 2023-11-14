package main

import (
    "io"
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"strings"
    "flag"
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

type Row struct {
    Id uint32
    Username [COLUMN_USERNAME_SIZE]byte
    Email [COLUMN_EMAIL_SIZE]byte
}
func NewRow(id uint32, username string, email string) (*Row) {
    row := Row{
        Id: id,
    }
    copy(row.Username[:], username)
    copy(row.Email[:], email)
    return &row
}
func (r *Row) String() string {
    emailLen := strLen(r.Email[:])
    userLen := strLen(r.Username[:])
    // should be a better way to do this
    return fmt.Sprintf(" %d | %s | %s", r.Id, string(r.Username[:userLen]), string(r.Email[:emailLen]))  
}

type Pager struct {
    FileDescriptor *os.File
    FileLength int64
    Pages [TABLE_MAX_PAGES]*[PAGE_SIZE]byte
}
func NewPager(filePath string) (*Pager, error) {
    if f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0755); err != nil {
        return nil, err
    } else {
        pager := Pager{
            FileDescriptor: f,
        }
        if stat, err := f.Stat(); err != nil {
            return nil, err
        } else {
            pager.FileLength = stat.Size()
        }

        return &pager, nil
    }
}
func (p *Pager) Flush(pageNum int, size int) (error) {
    if p.Pages[pageNum] == nil {
        return fmt.Errorf("Attempted to flush nil page %d", pageNum)
    }
    _, err := p.FileDescriptor.WriteAt(p.Pages[pageNum][:size], int64(pageNum * PAGE_SIZE))
    if err != nil {
        return err
    }
    
    return nil
}
func (p *Pager) GetPage(pageNum int) (*[PAGE_SIZE]byte, error) {
    if pageNum > TABLE_MAX_PAGES {
        return nil, fmt.Errorf("Page %d out of bounds %d", pageNum, TABLE_MAX_PAGES)
    }
    if p.Pages[pageNum] == nil {
        // TODO JH consider doing something with the returned offset instead of _ 
        if _, err := p.FileDescriptor.Seek(0, io.SeekStart); err != nil {
            return nil, err
        }
        page := make([]byte, PAGE_SIZE)
        p.FileDescriptor.Read(page)
        p.Pages[pageNum] = (*[PAGE_SIZE]byte)(page)
    }
    return p.Pages[pageNum], nil
}

type Table struct {
    RowCount uint32
    Pager *Pager
}
func OpenDb(filePath string) (*Table, error) {
    if pager, err := NewPager(filePath); err != nil {
        return nil, err
    } else {
        numRows := uint32(pager.FileLength / ROW_SIZE)
        table := Table{
            Pager: pager,
            RowCount: numRows,
        }
        return &table, nil
    }
}
func (t *Table) Close() error {
    pager := t.Pager
    fullPages := int(t.RowCount / ROWS_PER_PAGE)

    for i := 0; i < fullPages; i++ {
        if pager.Pages[i] == nil {
            continue
        }
        err := pager.Flush(i, PAGE_SIZE)
        if err != nil {
            return err
        }
    }
    numAdditionalRows := t.RowCount % ROWS_PER_PAGE
    if numAdditionalRows > 0 {
        if pager.Pages[fullPages] != nil {
            err := pager.Flush(fullPages, int(numAdditionalRows) * ROW_SIZE)
            if err != nil {
                return err
            }
        }    
    }
    return pager.FileDescriptor.Close()
}
func (t *Table) rowLocation(id int, alloc bool) (*[PAGE_SIZE]byte, uint, error) {
    pageIndex := id / ROWS_PER_PAGE
    page, err := t.Pager.GetPage(pageIndex)
    if err != nil {
        return nil, 0, err
    }
    rowInPage := id % ROWS_PER_PAGE
    pageOffset := rowInPage * ROW_SIZE
    return page, uint(pageOffset), nil
}
func (t *Table) Insert(row *Row) (error) {
    if t.RowCount >= uint32(TABLE_MAX_ROWS) {
        return fmt.Errorf("Table full.")
    }
    if page, offset, err := t.rowLocation(int(t.RowCount), true); err == nil {
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
    if page, offset, err := t.rowLocation(int(rowNum), false); err != nil {
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

func exec(dbFile string, in *io.Reader, out *io.Writer) {
    bufIn := bufio.NewReader(*in)
    bufOut := bufio.NewWriter(*out)
    // open table file
    table, err := OpenDb(dbFile)
    if err != nil {
        fmt.Printf("Error opening db %s: %s", dbFile, err.Error())
    }
    defer table.Close()
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
                    if row == nil {
                        panic(fmt.Sprintf("Row at %d was nil", i))
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
    dbFile := flag.String("dbfile", "dbfile.db", "Name of the database file")

    exec(*dbFile, &in, &out)
}
