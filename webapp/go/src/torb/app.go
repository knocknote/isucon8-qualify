package main

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/sessions"
	"github.com/labstack/echo"
	"github.com/labstack/echo-contrib/session"
	"github.com/labstack/echo/middleware"
	"go.knocknote.io/rapidash"
)

type User struct {
	ID        int64  `json:"id,omitempty"`
	Nickname  string `json:"nickname,omitempty"`
	LoginName string `json:"login_name,omitempty"`
	PassHash  string `json:"pass_hash,omitempty"`
}

func (u *User) EncodeRapidash(encoder rapidash.Encoder) error {
	if u.ID != 0 {
		encoder.Int64("id", u.ID)
	}
	encoder.String("nickname", u.Nickname)
	encoder.String("login_name", u.LoginName)
	encoder.String("pass_hash", u.PassHash)
	return encoder.Error()
}

func (u *User) DecodeRapidash(decoder rapidash.Decoder) error {
	u.ID = decoder.Int64("id")
	u.Nickname = decoder.String("nickname")
	u.LoginName = decoder.String("login_name")
	u.PassHash = decoder.String("pass_hash")
	return decoder.Error()
}

func (u User) RapidashStruct() *rapidash.Struct {
	return rapidash.NewStruct("users").
		FieldInt64("id").
		FieldString("nickname").
		FieldString("login_name").
		FieldString("pass_hash")
}

type Event struct {
	ID       int64  `json:"id,omitempty"`
	Title    string `json:"title,omitempty"`
	PublicFg bool   `json:"public,omitempty"`
	ClosedFg bool   `json:"closed,omitempty"`
	Price    int64  `json:"price,omitempty"`

	Total   int                `json:"total"`
	Remains int                `json:"remains"`
	Sheets  map[string]*Sheets `json:"sheets,omitempty"`
}

func (e *Event) DecodeRapidash(decoder rapidash.Decoder) error {
	e.ID = decoder.Int64("id")
	e.Title = decoder.String("title")
	e.PublicFg = decoder.Bool("public_fg")
	e.ClosedFg = decoder.Bool("closed_fg")
	e.Price = decoder.Int64("price")
	return decoder.Error()
}

func (e Event) RapidashStruct() *rapidash.Struct {
	return rapidash.NewStruct("events").
		FieldInt64("id").
		FieldString("title").
		FieldBool("public_fg").
		FieldBool("closed_fg").
		FieldInt64("price")
}

type EventSlice []*Event

func (e EventSlice) GroupByID() map[int64]*Event {
	eventMap := make(map[int64]*Event)
	for _, e := range e {
		eventMap[e.ID] = e
	}
	return eventMap
}

func (e *EventSlice) DecodeRapidash(decoder rapidash.Decoder) error {
	*e = make(EventSlice, decoder.Len())
	for i := 0; i < decoder.Len(); i++ {
		var event Event
		if err := event.DecodeRapidash(decoder.At(i)); err != nil {
			return err
		}
		(*e)[i] = &event
	}
	return decoder.Error()
}

func (e EventSlice) Filter(fn func(*Event) bool) EventSlice {
	events := make(EventSlice, 0, len(e))
	for _, event := range e {
		if fn(event) {
			events = append(events, event)
		}
	}
	return events
}

type Sheets struct {
	Total   int        `json:"total"`
	Remains int        `json:"remains"`
	Detail  SheetSlice `json:"detail,omitempty"`
	Price   int64      `json:"price"`
}

type Sheet struct {
	ID    int64  `json:"-"`
	Rank  string `json:"-"`
	Num   int64  `json:"num"`
	Price int64  `json:"-"`

	Mine           bool       `json:"mine,omitempty"`
	Reserved       bool       `json:"reserved,omitempty"`
	ReservedAt     *time.Time `json:"-"`
	ReservedAtUnix int64      `json:"reserved_at,omitempty"`
}

func (s *Sheet) DecodeRapidash(decoder rapidash.Decoder) error {
	s.ID = decoder.Int64("id")
	s.Rank = decoder.String("rank")
	s.Num = decoder.Int64("num")
	s.Price = decoder.Int64("price")

	return decoder.Error()
}

func (s Sheet) RapidashStruct() *rapidash.Struct {
	return rapidash.NewStruct("sheets").
		FieldInt64("id").
		FieldString("rank").
		FieldInt64("num").
		FieldInt64("price")
}

type SheetSlice []*Sheet

func (s SheetSlice) GroupByID() map[int64]*Sheet {
	sheetMap := make(map[int64]*Sheet)
	for _, s := range s {
		sheetMap[s.ID] = s
	}
	return sheetMap
}

func (s SheetSlice) IDs() []int64 {
	ids := make([]int64, len(s))
	for i, s := range s {
		ids[i] = s.ID
	}
	return ids
}

func (s *SheetSlice) DecodeRapidash(decoder rapidash.Decoder) error {
	*s = make([]*Sheet, decoder.Len())
	for i := 0; i < decoder.Len(); i++ {
		var sheet Sheet
		if err := sheet.DecodeRapidash(decoder.At(i)); err != nil {
			return err
		}
		(*s)[i] = &sheet
	}
	return decoder.Error()
}

type Reservation struct {
	ID         int64      `json:"id"`
	EventID    int64      `json:"-"`
	SheetID    int64      `json:"-"`
	UserID     int64      `json:"-"`
	ReservedAt *time.Time `json:"-"`
	CanceledAt *time.Time `json:"-"`

	Event          *Event `json:"event,omitempty"`
	SheetRank      string `json:"sheet_rank,omitempty"`
	SheetNum       int64  `json:"sheet_num,omitempty"`
	Price          int64  `json:"price,omitempty"`
	ReservedAtUnix int64  `json:"reserved_at,omitempty"`
	CanceledAtUnix int64  `json:"canceled_at,omitempty"`
}

func (r *Reservation) EncodeRapidash(encoder rapidash.Encoder) error {
	if r.ID != 0 {
		encoder.Int64("id", r.ID)
	}
	encoder.Int64("event_id", r.EventID)
	encoder.Int64("sheet_id", r.SheetID)
	encoder.Int64("user_id", r.UserID)
	encoder.TimePtr("reserved_at", r.ReservedAt)
	encoder.TimePtr("canceled_at", r.CanceledAt)
	return encoder.Error()
}

func (r *Reservation) DecodeRapidash(decoder rapidash.Decoder) error {
	r.ID = decoder.Int64("id")
	r.EventID = decoder.Int64("event_id")
	r.SheetID = decoder.Int64("sheet_id")
	r.UserID = decoder.Int64("user_id")
	r.ReservedAt = decoder.TimePtr("reserved_at")
	r.CanceledAt = decoder.TimePtr("canceled_at")

	return decoder.Error()
}

func (r Reservation) RapidashStruct() *rapidash.Struct {
	return rapidash.NewStruct("reservations").
		FieldInt64("id").
		FieldInt64("event_id").
		FieldInt64("sheet_id").
		FieldInt64("user_id").
		FieldTime("reserved_at").
		FieldTime("canceled_at")
}

type ReservationSlice []*Reservation

func (r ReservationSlice) SheetIDs() []int64 {
	ids := make([]int64, len(r))
	for i, r := range r {
		ids[i] = r.SheetID
	}
	return ids
}

func (r ReservationSlice) EventIDs() []int64 {
	ids := make([]int64, len(r))
	for i, r := range r {
		ids[i] = r.EventID
	}
	return ids
}

func (r ReservationSlice) Sort() {
	sort.Slice(r, func(i, j int) bool {
		timeA, timeB := r[i].CanceledAt, r[j].CanceledAt
		if timeA == nil {
			timeA = r[i].ReservedAt
		}
		if timeB == nil {
			timeB = r[j].ReservedAt
		}
		return timeA.After(*timeB)
	})
}

func (r *ReservationSlice) DecodeRapidash(decoder rapidash.Decoder) error {
	*r = make(ReservationSlice, decoder.Len())
	for i := 0; i < decoder.Len(); i++ {
		decoder := decoder.At(i)
		(*r)[i] = &Reservation{
			ID:         decoder.Int64("id"),
			EventID:    decoder.Int64("event_id"),
			SheetID:    decoder.Int64("sheet_id"),
			UserID:     decoder.Int64("user_id"),
			ReservedAt: decoder.TimePtr("reserved_at"),
			CanceledAt: decoder.TimePtr("canceled_at"),
		}
	}
	return decoder.Error()
}

type Administrator struct {
	ID        int64  `json:"id,omitempty"`
	Nickname  string `json:"nickname,omitempty"`
	LoginName string `json:"login_name,omitempty"`
	PassHash  string `json:"pass_hash,omitempty"`
}

func (a *Administrator) DecodeRapidash(decoder rapidash.Decoder) error {
	a.ID = decoder.Int64("id")
	a.Nickname = decoder.String("nickname")
	a.LoginName = decoder.String("login_name")
	a.PassHash = decoder.String("pass_hash")
	return decoder.Error()
}

func (a Administrator) RapidashStruct() *rapidash.Struct {
	return rapidash.NewStruct("administrators").
		FieldInt64("id").
		FieldString("nickname").
		FieldString("login_name").
		FieldString("pass_hash")
}

func sessUserID(c echo.Context) int64 {
	sess, _ := session.Get("session", c)
	var userID int64
	if x, ok := sess.Values["user_id"]; ok {
		userID, _ = x.(int64)
	}
	return userID
}

func sessSetUserID(c echo.Context, id int64) {
	sess, _ := session.Get("session", c)
	sess.Options = &sessions.Options{
		Path:     "/",
		MaxAge:   3600,
		HttpOnly: true,
	}
	sess.Values["user_id"] = id
	sess.Save(c.Request(), c.Response())
}

func sessDeleteUserID(c echo.Context) {
	sess, _ := session.Get("session", c)
	sess.Options = &sessions.Options{
		Path:     "/",
		MaxAge:   3600,
		HttpOnly: true,
	}
	delete(sess.Values, "user_id")
	sess.Save(c.Request(), c.Response())
}

func sessAdministratorID(c echo.Context) int64 {
	sess, _ := session.Get("session", c)
	var administratorID int64
	if x, ok := sess.Values["administrator_id"]; ok {
		administratorID, _ = x.(int64)
	}
	return administratorID
}

func sessSetAdministratorID(c echo.Context, id int64) {
	sess, _ := session.Get("session", c)
	sess.Options = &sessions.Options{
		Path:     "/",
		MaxAge:   3600,
		HttpOnly: true,
	}
	sess.Values["administrator_id"] = id
	sess.Save(c.Request(), c.Response())
}

func sessDeleteAdministratorID(c echo.Context) {
	sess, _ := session.Get("session", c)
	sess.Options = &sessions.Options{
		Path:     "/",
		MaxAge:   3600,
		HttpOnly: true,
	}
	delete(sess.Values, "administrator_id")
	sess.Save(c.Request(), c.Response())
}

func loginRequired(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		cacheTx, err := cache.Begin(db)
		if err != nil {
			return err
		}
		defer func() {
			cacheTx.RollbackUnlessCommitted()
		}()
		if _, err := getLoginUser(cacheTx, c); err != nil {
			return resError(c, "login_required", 401)
		}
		if err := cacheTx.Commit(); err != nil {
			return err
		}
		return next(c)
	}
}

func adminLoginRequired(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		cacheTx, err := cache.Begin(db)
		if err != nil {
			return err
		}
		defer func() {
			cacheTx.RollbackUnlessCommitted()
		}()
		if _, err := getLoginAdministrator(cacheTx, c); err != nil {
			return resError(c, "admin_login_required", 401)
		}
		if err := cacheTx.Commit(); err != nil {
			return err
		}
		return next(c)
	}
}

func getLoginUser(tx *rapidash.Tx, c echo.Context) (*User, error) {
	userID := sessUserID(c)
	if userID == 0 {
		return nil, errors.New("not logged in")
	}

	var user User
	if err := tx.FindByQueryBuilder(rapidash.NewQueryBuilder("users").Eq("id", userID), &user); err != nil {
		return nil, err
	}

	return &user, nil
}

func getLoginAdministrator(tx *rapidash.Tx, c echo.Context) (*Administrator, error) {
	administratorID := sessAdministratorID(c)
	if administratorID == 0 {
		return nil, errors.New("not logged in")
	}
	var administrator Administrator
	if err := tx.FindByQueryBuilder(rapidash.NewQueryBuilder("administrators").Eq("id", administratorID), &administrator); err != nil {
		return nil, err
	}
	return &administrator, nil
}

func getEvents(tx *rapidash.Tx, all bool) ([]*Event, error) {
	var events EventSlice
	query := rapidash.NewQueryBuilder("events").OrderAsc("id")
	if err := tx.FindByQueryBuilder(query, &events); err != nil {
		return nil, err
	}
	if !all {
		events = events.Filter(func(event *Event) bool {
			return event.PublicFg
		})
	}
	for i, v := range events {
		event, err := getEvent(tx, v.ID, -1)
		if err != nil {
			return nil, err
		}
		for k := range event.Sheets {
			event.Sheets[k].Detail = nil
		}
		events[i] = event
	}
	return events, nil
}

func getEvent(tx *rapidash.Tx, eventID, loginUserID int64) (*Event, error) {
	var event Event
	if err := tx.FindByQueryBuilder(rapidash.NewQueryBuilder("events").Eq("id", eventID), &event); err != nil {
		return nil, err
	}
	if event.ID == 0 {
		return nil, sql.ErrNoRows
	}
	event.Sheets = map[string]*Sheets{
		"S": &Sheets{},
		"A": &Sheets{},
		"B": &Sheets{},
		"C": &Sheets{},
	}

	var sheets SheetSlice
	if err := tx.FindByQueryBuilder(rapidash.NewQueryBuilder("sheets").OrderBy("rank").OrderBy("num"), &sheets); err != nil {
		return nil, err
	}
	var reservations ReservationSlice
	if err := tx.FindByQueryBuilder(rapidash.NewQueryBuilder("reservations").Eq("event_id", event.ID).Eq("canceled_at", nil).In("sheet_id", sheets.IDs()), &reservations); err != nil {
		return nil, err
	}
	reservationsMap := make(map[int64]ReservationSlice)
	for _, reservation := range reservations {
		reservationsMap[reservation.SheetID] = append(reservationsMap[reservation.SheetID], reservation)
	}
	for _, sheet := range sheets {
		reservations := reservationsMap[sheet.ID]
		event.Sheets[sheet.Rank].Price = event.Price + sheet.Price
		event.Total++
		event.Sheets[sheet.Rank].Total++
		if len(reservations) == 0 {
			event.Remains++
			event.Sheets[sheet.Rank].Remains++
		} else {
			var reservation *Reservation
			for _, r := range reservations {
				if reservation == nil {
					reservation = r
				} else if reservation.ReservedAt.After(*r.ReservedAt) {
					reservation = r
				}
			}
			sheet.Mine = reservation.UserID == loginUserID
			sheet.Reserved = true
			sheet.ReservedAtUnix = reservation.ReservedAt.Unix()
		}
		event.Sheets[sheet.Rank].Detail = append(event.Sheets[sheet.Rank].Detail, sheet)
	}

	return &event, nil
}

func sanitizeEvent(e *Event) *Event {
	sanitized := *e
	sanitized.Price = 0
	sanitized.PublicFg = false
	sanitized.ClosedFg = false
	return &sanitized
}

func fillinUser(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		cacheTx, err := cache.Begin(db)
		if err != nil {
			return err
		}
		defer func() {
			cacheTx.RollbackUnlessCommitted()
		}()
		if user, err := getLoginUser(cacheTx, c); err == nil {
			c.Set("user", user)
		}
		if err := cacheTx.Commit(); err != nil {
			return err
		}
		return next(c)
	}
}

func fillinAdministrator(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		cacheTx, err := cache.Begin(db)
		if err != nil {
			return err
		}
		defer func() {
			cacheTx.RollbackUnlessCommitted()
		}()

		if administrator, err := getLoginAdministrator(cacheTx, c); err == nil {
			c.Set("administrator", administrator)
		}
		if err := cacheTx.Commit(); err != nil {
			return err
		}
		return next(c)
	}
}

func validateRank(tx *rapidash.Tx, rank string) bool {
	query := rapidash.NewQueryBuilder("sheets").Eq("rank", rank)
	count, _ := tx.CountByQueryBuilder(query)
	return count > 0
}

type Renderer struct {
	templates *template.Template
}

func (r *Renderer) Render(w io.Writer, name string, data interface{}, c echo.Context) error {
	return r.templates.ExecuteTemplate(w, name, data)
}

var (
	db    *sql.DB
	cache *rapidash.Rapidash

	readOnlyTables = map[string]*rapidash.Struct{
		"administrators": Administrator{}.RapidashStruct(),
		"sheets":         Sheet{}.RapidashStruct(),
		"events":         Event{}.RapidashStruct(),
	}
	readWriteTables = map[string]*rapidash.Struct{
		"users":        User{}.RapidashStruct(),
		"reservations": Reservation{}.RapidashStruct(),
	}
)

func main() {
	e := echo.New()
	funcs := template.FuncMap{
		"encode_json": func(v interface{}) string {
			b, _ := json.Marshal(v)
			return string(b)
		},
	}
	e.Renderer = &Renderer{
		templates: template.Must(template.New("").Delims("[[", "]]").Funcs(funcs).ParseGlob("views/*.tmpl")),
	}
	e.Use(session.Middleware(sessions.NewCookieStore([]byte("secret"))))
	e.Use(middleware.LoggerWithConfig(middleware.LoggerConfig{Output: os.Stderr}))
	e.Static("/", "public")
	e.GET("/", func(c echo.Context) error {
		cacheTx, err := cache.Begin(db)
		if err != nil {
			return err
		}
		defer func() {
			cacheTx.RollbackUnlessCommitted()
		}()
		events, err := getEvents(cacheTx, false)
		if err != nil {
			return err
		}
		for i, v := range events {
			events[i] = sanitizeEvent(v)
		}
		if err := cacheTx.Commit(); err != nil {
			return err
		}
		return c.Render(200, "index.tmpl", echo.Map{
			"events": events,
			"user":   c.Get("user"),
			"origin": c.Scheme() + "://" + c.Request().Host,
		})
	}, fillinUser)
	e.GET("/initialize", func(c echo.Context) error {
		db.Close()
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true&charset=utf8mb4",
			os.Getenv("DB_USER"), os.Getenv("DB_PASS"),
			os.Getenv("DB_HOST"), os.Getenv("DB_PORT"),
			os.Getenv("DB_DATABASE"),
		)

		var err error
		db, err = sql.Open("mysql", dsn)
		if err != nil {
			return err
		}
		cmd := exec.Command("../../db/init.sh")
		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stdout
		if err := cmd.Run(); err != nil {
			return err
		}
		if err := cache.Flush(); err != nil {
			return err
		}
		if err := warmUp(); err != nil {
			return err
		}
		return c.NoContent(204)
	})
	e.POST("/api/users", func(c echo.Context) error {
		var params struct {
			Nickname  string `json:"nickname"`
			LoginName string `json:"login_name"`
			Password  string `json:"password"`
		}
		c.Bind(&params)

		tx, err := db.Begin()
		if err != nil {
			return err
		}
		cacheTx, err := cache.Begin(tx)
		if err != nil {
			return err
		}
		defer func() {
			if err := cacheTx.RollbackUnlessCommitted(); err != nil {
				log.Println(err)
			}
		}()

		{
			var user User
			if err := cacheTx.FindByQueryBuilder(rapidash.NewQueryBuilder("users").Eq("login_name", params.LoginName), &user); err != nil {
				return err
			}
			if user.ID != 0 {
				return resError(c, "duplicated", 409)
			}
		}

		user := &User{
			LoginName: params.LoginName,
			PassHash:  fmt.Sprintf("%x", sha256.Sum256([]byte(params.Password))),
			Nickname:  params.Nickname,
		}

		id, err := cacheTx.CreateByTable("users", user)
		if err != nil {
			return resError(c, "", 0)
		}

		if err := cacheTx.Commit(); err != nil {
			return err
		}

		return c.JSON(201, echo.Map{
			"id":       id,
			"nickname": params.Nickname,
		})
	})
	e.GET("/api/users/:id", func(c echo.Context) error {
		var user User

		cacheTx, err := cache.Begin(db)
		if err != nil {
			return err
		}
		defer func() {
			if err := cacheTx.RollbackUnlessCommitted(); err != nil {
				log.Println(err)
			}
		}()
		userID, _ := strconv.ParseInt(c.Param("id"), 10, 64)

		if err := cacheTx.FindByQueryBuilder(rapidash.NewQueryBuilder("users").Eq("id", userID), &user); err != nil {
			return err
		}

		loginUser, err := getLoginUser(cacheTx, c)
		if err != nil {
			return err
		}
		if user.ID != loginUser.ID {
			return resError(c, "forbidden", 403)
		}

		var recentReservations ReservationSlice
		if err := cacheTx.FindByQueryBuilder(rapidash.NewQueryBuilder("reservations").Eq("user_id", user.ID), &recentReservations); err != nil {
			return err
		}
		if recentReservations == nil {
			recentReservations = make(ReservationSlice, 0)
		}
		recentReservations.Sort()
		limitLen := 5
		if len(recentReservations) < limitLen {
			limitLen = len(recentReservations)
		}
		recentReservations = recentReservations[:limitLen]
		var sheets SheetSlice
		query := rapidash.NewQueryBuilder("sheets").In("id", recentReservations.SheetIDs())
		if err := cacheTx.FindByQueryBuilder(query, &sheets); err != nil {
			return err
		}
		sheetMap := sheets.GroupByID()
		for _, reservation := range recentReservations {
			sheet := sheetMap[reservation.SheetID]
			event, err := getEvent(cacheTx, reservation.EventID, -1)
			if err != nil {
				return err
			}
			price := event.Sheets[sheet.Rank].Price
			event.Sheets = nil
			event.Total = 0
			event.Remains = 0

			reservation.Event = event
			reservation.SheetRank = sheet.Rank
			reservation.SheetNum = sheet.Num
			reservation.Price = price
			reservation.ReservedAtUnix = reservation.ReservedAt.Unix()
			if reservation.CanceledAt != nil {
				reservation.CanceledAtUnix = reservation.CanceledAt.Unix()
			}
		}

		var totalPrice int
		{
			var reservations ReservationSlice
			if err := cacheTx.FindByQueryBuilder(rapidash.NewQueryBuilder("reservations").Eq("user_id", user.ID).Eq("canceled_at", nil), &reservations); err != nil {
				return err
			}
			var sheets SheetSlice
			if err := cacheTx.FindByQueryBuilder(rapidash.NewQueryBuilder("sheets").In("id", reservations.SheetIDs()), &sheets); err != nil {
				return err
			}
			for _, sheet := range sheets {
				totalPrice += int(sheet.Price)
			}
			var events EventSlice
			if err := cacheTx.FindByQueryBuilder(rapidash.NewQueryBuilder("events").In("id", reservations.EventIDs()), &events); err != nil {
				return err
			}
			for _, event := range events {
				totalPrice += int(event.Price)
			}
		}

		var recentEvents []*Event
		{
			var reservations ReservationSlice
			if err := cacheTx.FindByQueryBuilder(rapidash.NewQueryBuilder("reservations").Eq("user_id", user.ID), &reservations); err != nil {
				return err
			}
			reservations.Sort()
			eventIDMap := map[int64]struct{}{}
			for _, reservation := range reservations {
				if _, exists := eventIDMap[reservation.EventID]; exists {
					continue
				}
				eventIDMap[reservation.EventID] = struct{}{}
				event, err := getEvent(cacheTx, reservation.EventID, -1)
				if err != nil {
					return err
				}
				for k := range event.Sheets {
					event.Sheets[k].Detail = nil
				}
				recentEvents = append(recentEvents, event)
				if len(recentEvents) == 5 {
					break
				}
			}
			if recentEvents == nil {
				recentEvents = make([]*Event, 0)
			}
		}

		if err := cacheTx.Commit(); err != nil {
			return err
		}

		return c.JSON(200, echo.Map{
			"id":                  user.ID,
			"nickname":            user.Nickname,
			"recent_reservations": recentReservations,
			"total_price":         totalPrice,
			"recent_events":       recentEvents,
		})
	}, loginRequired)
	e.POST("/api/actions/login", func(c echo.Context) error {
		var params struct {
			LoginName string `json:"login_name"`
			Password  string `json:"password"`
		}
		c.Bind(&params)

		cacheTx, err := cache.Begin(db)
		if err != nil {
			return err
		}
		defer func() {
			if err := cacheTx.RollbackUnlessCommitted(); err != nil {
				log.Println(err)
			}
		}()
		var user User
		if err := cacheTx.FindByQueryBuilder(rapidash.NewQueryBuilder("users").Eq("login_name", params.LoginName), &user); err != nil {
			return err
		}
		if user.ID == 0 {
			return resError(c, "authentication_failed", 401)
		}

		if user.PassHash != fmt.Sprintf("%x", sha256.Sum256([]byte(params.Password))) {
			return resError(c, "authentication_failed", 401)
		}
		sessSetUserID(c, user.ID)
		u, err := getLoginUser(cacheTx, c)
		if err != nil {
			return err
		}

		if err := cacheTx.Commit(); err != nil {
			return err
		}

		return c.JSON(200, u)
	})
	e.POST("/api/actions/logout", func(c echo.Context) error {
		sessDeleteUserID(c)
		return c.NoContent(204)
	}, loginRequired)
	e.GET("/api/events", func(c echo.Context) error {
		cacheTx, err := cache.Begin(db)
		if err != nil {
			return err
		}
		defer func() {
			cacheTx.RollbackUnlessCommitted()
		}()
		events, err := getEvents(cacheTx, true)
		if err != nil {
			return err
		}
		for i, v := range events {
			events[i] = sanitizeEvent(v)
		}
		if err := cacheTx.Commit(); err != nil {
			return err
		}
		return c.JSON(200, events)
	})
	e.GET("/api/events/:id", func(c echo.Context) error {
		cacheTx, err := cache.Begin(db)
		if err != nil {
			return err
		}
		defer func() {
			cacheTx.RollbackUnlessCommitted()
		}()
		eventID, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			return resError(c, "not_found", 404)
		}

		loginUserID := int64(-1)
		if user, err := getLoginUser(cacheTx, c); err == nil {
			loginUserID = user.ID
		}

		event, err := getEvent(cacheTx, eventID, loginUserID)
		if err != nil {
			if err == sql.ErrNoRows {
				return resError(c, "not_found", 404)
			}
			return err
		} else if !event.PublicFg {
			return resError(c, "not_found", 404)
		}

		if err := cacheTx.Commit(); err != nil {
			return err
		}
		return c.JSON(200, sanitizeEvent(event))
	})
	e.POST("/api/events/:id/actions/reserve", func(c echo.Context) error {
		cacheTx, err := cache.Begin(db)
		if err != nil {
			return err
		}
		defer func() {
			if err := cacheTx.RollbackUnlessCommitted(); err != nil {
				log.Println(err)
			}
		}()

		eventID, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			return resError(c, "not_found", 404)
		}
		var params struct {
			Rank string `json:"sheet_rank"`
		}
		c.Bind(&params)

		user, err := getLoginUser(cacheTx, c)
		if err != nil {
			return err
		}

		event, err := getEvent(cacheTx, eventID, user.ID)
		if err != nil {
			if err == sql.ErrNoRows {
				return resError(c, "invalid_event", 404)
			}
			return err
		} else if !event.PublicFg {
			return resError(c, "invalid_event", 404)
		}

		if !validateRank(cacheTx, params.Rank) {
			return resError(c, "invalid_rank", 400)
		}

		var sheets SheetSlice
		builder := rapidash.NewQueryBuilder("sheets").Eq("rank", params.Rank)
		if err := cacheTx.FindByQueryBuilder(builder, &sheets); err != nil {
			return err
		}

		rand.Shuffle(len(sheets), func(i, j int) {
			sheets[i], sheets[j] = sheets[j], sheets[i]
		})

		if err := cacheTx.Commit(); err != nil {
			return err
		}

		findReservation := func(tx *rapidash.Tx) (map[int64]struct{}, error) {
			reservationMap := make(map[int64]struct{})

			var reservations ReservationSlice
			if err := tx.FindByQueryBuilder(rapidash.NewQueryBuilder("reservations").Eq("event_id", event.ID).Eq("canceled_at", nil), &reservations); err != nil {
				return nil, err
			}
			for _, reservation := range reservations {
				reservationMap[reservation.SheetID] = struct{}{}
			}
			return reservationMap, nil
		}

		var sheet *Sheet
		var reservationID int64
		for {

			tx, err := db.Begin()
			if err != nil {
				return err
			}
			cacheTx, err := cache.Begin(tx)
			if err != nil {
				return err
			}
			reservations, err := findReservation(cacheTx)
			if err != nil {
				return err
			}

			if len(sheets) == len(reservations) {
				return resError(c, "sold_out", 409)
			}

			for _, s := range sheets {
				if _, exists := reservations[s.ID]; !exists {
					sheet = s
					break
				}
			}

			now := time.Now().UTC()
			reservation := &Reservation{
				EventID:    event.ID,
				SheetID:    sheet.ID,
				UserID:     user.ID,
				ReservedAt: &now,
			}
			reservationID, err = cacheTx.CreateByTable("reservations", reservation)
			if err != nil {
				cacheTx.Rollback()
				log.Println("re-try: rollback by", err)
				continue
			}
			if err := cacheTx.Commit(); err != nil {
				cacheTx.Rollback()
				log.Println("re-try: rollback by", err)
				continue
			}
			break
		}

		return c.JSON(202, echo.Map{
			"id":         reservationID,
			"sheet_rank": params.Rank,
			"sheet_num":  sheet.Num,
		})
	}, loginRequired)
	e.DELETE("/api/events/:id/sheets/:rank/:num/reservation", func(c echo.Context) error {
		tx, err := db.Begin()
		if err != nil {
			return err
		}
		cacheTx, err := cache.Begin(tx)
		if err != nil {
			return err
		}
		defer func() {
			if err := cacheTx.RollbackUnlessCommitted(); err != nil {
				log.Println(err)
			}
		}()
		eventID, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			return resError(c, "not_found", 404)
		}
		rank := c.Param("rank")

		numStr := c.Param("num")
		num, _ := strconv.ParseInt(numStr, 10, 64)

		user, err := getLoginUser(cacheTx, c)
		if err != nil {
			return err
		}

		event, err := getEvent(cacheTx, eventID, user.ID)
		if err != nil {
			if err == sql.ErrNoRows {
				return resError(c, "invalid_event", 404)
			}
			return err
		} else if !event.PublicFg {
			return resError(c, "invalid_event", 404)
		}

		if !validateRank(cacheTx, rank) {
			return resError(c, "invalid_rank", 404)
		}

		var sheet Sheet
		query := rapidash.NewQueryBuilder("sheets").Eq("rank", rank).Eq("num", num)
		if err := cacheTx.FindByQueryBuilder(query, &sheet); err != nil {
			return err
		}

		if sheet.ID == 0 {
			return resError(c, "invalid_sheet", 404)
		}

		var reservations ReservationSlice
		if err := cacheTx.FindByQueryBuilder(rapidash.NewQueryBuilder("reservations").Eq("event_id", event.ID).Eq("canceled_at", nil).Eq("sheet_id", sheet.ID), &reservations); err != nil {
			return err
		}
		if len(reservations) == 0 {
			return resError(c, "not_reserved", 400)
		}
		var reservation *Reservation
		for _, r := range reservations {
			if reservation == nil {
				reservation = r
			} else if reservation.ReservedAt.After(*r.ReservedAt) {
				reservation = r
			}
		}

		if reservation.UserID != user.ID {
			return resError(c, "not_permitted", 403)
		}

		if err := cacheTx.UpdateByQueryBuilder(rapidash.NewQueryBuilder("reservations").Eq("id", reservation.ID), map[string]interface{}{"canceled_at": time.Now().UTC()}); err != nil {
			return err
		}

		if err := cacheTx.Commit(); err != nil {
			return err
		}

		return c.NoContent(204)
	}, loginRequired)
	e.GET("/admin/", func(c echo.Context) error {
		var events []*Event
		administrator := c.Get("administrator")
		if administrator != nil {
			cacheTx, err := cache.Begin(db)
			if err != nil {
				return err
			}
			defer func() {
				cacheTx.RollbackUnlessCommitted()
			}()
			if events, err = getEvents(cacheTx, true); err != nil {
				return err
			}
			if err := cacheTx.Commit(); err != nil {
				return err
			}
		}
		return c.Render(200, "admin.tmpl", echo.Map{
			"events":        events,
			"administrator": administrator,
			"origin":        c.Scheme() + "://" + c.Request().Host,
		})
	}, fillinAdministrator)
	e.POST("/admin/api/actions/login", func(c echo.Context) error {
		var params struct {
			LoginName string `json:"login_name"`
			Password  string `json:"password"`
		}
		c.Bind(&params)

		cacheTx, err := cache.Begin(db)
		if err != nil {
			return err
		}
		defer func() {
			if err := cacheTx.RollbackUnlessCommitted(); err != nil {
				log.Println(err)
			}
		}()

		var administrator Administrator
		if err := cacheTx.FindByQueryBuilder(rapidash.NewQueryBuilder("administrators").Eq("login_name", params.LoginName), &administrator); err != nil {
			return err
		}
		if administrator.ID == 0 {
			return resError(c, "authentication_failed", 401)
		}

		if administrator.PassHash != fmt.Sprintf("%x", sha256.Sum256([]byte(params.Password))) {
			return resError(c, "authentication_failed", 401)
		}

		sessSetAdministratorID(c, administrator.ID)
		loginAdministrator, err := getLoginAdministrator(cacheTx, c)
		if err != nil {
			return err
		}
		if err := cacheTx.Commit(); err != nil {
			return err
		}
		return c.JSON(200, loginAdministrator)
	})
	e.POST("/admin/api/actions/logout", func(c echo.Context) error {
		sessDeleteAdministratorID(c)
		return c.NoContent(204)
	}, adminLoginRequired)
	e.GET("/admin/api/events", func(c echo.Context) error {
		cacheTx, err := cache.Begin(db)
		if err != nil {
			return err
		}
		defer func() {
			cacheTx.RollbackUnlessCommitted()
		}()
		events, err := getEvents(cacheTx, true)
		if err != nil {
			return err
		}
		if err := cacheTx.Commit(); err != nil {
			return err
		}
		return c.JSON(200, events)
	}, adminLoginRequired)
	e.POST("/admin/api/events", func(c echo.Context) error {
		var params struct {
			Title  string `json:"title"`
			Public bool   `json:"public"`
			Price  int    `json:"price"`
		}
		c.Bind(&params)

		tx, err := db.Begin()
		if err != nil {
			return err
		}

		res, err := tx.Exec("INSERT INTO events (title, public_fg, closed_fg, price) VALUES (?, ?, 0, ?)", params.Title, params.Public, params.Price)
		if err != nil {
			tx.Rollback()
			return err
		}
		eventID, err := res.LastInsertId()
		if err != nil {
			tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}

		if err := cache.WarmUpFirstLevelCache(db, readOnlyTables["events"]); err != nil {
			return err
		}

		cacheTx, err := cache.Begin(db)
		if err != nil {
			return err
		}
		defer func() {
			cacheTx.RollbackUnlessCommitted()
		}()

		event, err := getEvent(cacheTx, eventID, -1)
		if err != nil {
			return err
		}

		if err := cacheTx.Commit(); err != nil {
			return err
		}

		return c.JSON(200, event)
	}, adminLoginRequired)
	e.GET("/admin/api/events/:id", func(c echo.Context) error {
		cacheTx, err := cache.Begin(db)
		if err != nil {
			return err
		}
		defer func() {
			cacheTx.RollbackUnlessCommitted()
		}()
		eventID, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			return resError(c, "not_found", 404)
		}
		event, err := getEvent(cacheTx, eventID, -1)
		if err != nil {
			if err == sql.ErrNoRows {
				return resError(c, "not_found", 404)
			}
			return err
		}
		if err := cacheTx.Commit(); err != nil {
			return err
		}
		return c.JSON(200, event)
	}, adminLoginRequired)
	e.POST("/admin/api/events/:id/actions/edit", func(c echo.Context) error {
		cacheTx, err := cache.Begin(db)
		if err != nil {
			return err
		}
		defer func() {
			cacheTx.RollbackUnlessCommitted()
		}()
		eventID, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			return resError(c, "not_found", 404)
		}

		var params struct {
			Public bool `json:"public"`
			Closed bool `json:"closed"`
		}
		c.Bind(&params)
		if params.Closed {
			params.Public = false
		}

		event, err := getEvent(cacheTx, eventID, -1)
		if err != nil {
			if err == sql.ErrNoRows {
				return resError(c, "not_found", 404)
			}
			return err
		}

		if event.ClosedFg {
			return resError(c, "cannot_edit_closed_event", 400)
		} else if event.PublicFg && params.Closed {
			return resError(c, "cannot_close_public_event", 400)
		}

		if err := cacheTx.Commit(); err != nil {
			return err
		}

		tx, err := db.Begin()
		if err != nil {
			return err
		}
		if _, err := tx.Exec("UPDATE events SET public_fg = ?, closed_fg = ? WHERE id = ?", params.Public, params.Closed, event.ID); err != nil {
			tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}

		if err := cache.WarmUpFirstLevelCache(db, readOnlyTables["events"]); err != nil {
			return err
		}

		event.PublicFg = params.Public
		event.ClosedFg = params.Closed
		c.JSON(200, event)
		return nil
	}, adminLoginRequired)
	e.GET("/admin/api/reports/events/:id/sales", func(c echo.Context) error {
		cacheTx, err := cache.Begin(db)
		if err != nil {
			return err
		}
		defer func() {
			if err := cacheTx.RollbackUnlessCommitted(); err != nil {
				log.Println(err)
			}
		}()

		eventID, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			return resError(c, "not_found", 404)
		}

		event, err := getEvent(cacheTx, eventID, -1)
		if err != nil {
			return err
		}

		var reservations ReservationSlice
		if err := cacheTx.FindByQueryBuilder(rapidash.NewQueryBuilder("reservations").Eq("event_id", event.ID).OrderAsc("reserved_at"), &reservations); err != nil {
			return err
		}

		var sheets SheetSlice
		if err := cacheTx.FindByQueryBuilder(rapidash.NewQueryBuilder("sheets").In("id", reservations.SheetIDs()), &sheets); err != nil {
			return err
		}
		sheetMap := sheets.GroupByID()

		var reports []Report
		for _, reservation := range reservations {
			sheet := sheetMap[reservation.SheetID]
			report := Report{
				ReservationID: reservation.ID,
				EventID:       event.ID,
				Rank:          sheet.Rank,
				Num:           sheet.Num,
				UserID:        reservation.UserID,
				SoldAt:        reservation.ReservedAt.Format("2006-01-02T15:04:05.000000Z"),
				Price:         event.Price + sheet.Price,
			}
			if reservation.CanceledAt != nil {
				report.CanceledAt = reservation.CanceledAt.Format("2006-01-02T15:04:05.000000Z")
			}
			reports = append(reports, report)
		}

		if err := cacheTx.Commit(); err != nil {
			return err
		}

		return renderReportCSV(c, reports)
	}, adminLoginRequired)
	e.GET("/admin/api/reports/sales", func(c echo.Context) error {
		cacheTx, err := cache.Begin(db)
		if err != nil {
			return err
		}
		defer func() {
			if err := cacheTx.RollbackUnlessCommitted(); err != nil {
				log.Println(err)
			}
		}()

		rows, err := db.Query("SELECT * FROM reservations ORDER BY reserved_at ASC")
		if err != nil {
			return err
		}
		defer rows.Close()

		var sheets SheetSlice
		if err := cacheTx.FindAllByTable("sheets", &sheets); err != nil {
			return err
		}
		sheetMap := sheets.GroupByID()
		var events EventSlice
		if err := cacheTx.FindAllByTable("events", &events); err != nil {
			return err
		}
		eventMap := events.GroupByID()
		var reports []Report
		for rows.Next() {
			var reservation Reservation
			if err := rows.Scan(&reservation.ID, &reservation.EventID, &reservation.SheetID, &reservation.UserID, &reservation.ReservedAt, &reservation.CanceledAt); err != nil {
				return err
			}

			sheet := sheetMap[reservation.SheetID]
			event := eventMap[reservation.EventID]
			report := Report{
				ReservationID: reservation.ID,
				EventID:       event.ID,
				Rank:          sheet.Rank,
				Num:           sheet.Num,
				UserID:        reservation.UserID,
				SoldAt:        reservation.ReservedAt.Format("2006-01-02T15:04:05.000000Z"),
				Price:         event.Price + sheet.Price,
			}
			if reservation.CanceledAt != nil {
				report.CanceledAt = reservation.CanceledAt.Format("2006-01-02T15:04:05.000000Z")
			}
			reports = append(reports, report)
		}

		if err := cacheTx.Commit(); err != nil {
			return err
		}
		return renderReportCSV(c, reports)
	}, adminLoginRequired)
	e.Start(":8080")
}

type Report struct {
	ReservationID int64
	EventID       int64
	Rank          string
	Num           int64
	UserID        int64
	SoldAt        string
	CanceledAt    string
	Price         int64
}

func renderReportCSV(c echo.Context, reports []Report) error {
	sort.Slice(reports, func(i, j int) bool { return strings.Compare(reports[i].SoldAt, reports[j].SoldAt) < 0 })

	body := bytes.NewBufferString("reservation_id,event_id,rank,num,price,user_id,sold_at,canceled_at\n")
	for _, v := range reports {
		body.WriteString(fmt.Sprintf("%d,%d,%s,%d,%d,%d,%s,%s\n",
			v.ReservationID, v.EventID, v.Rank, v.Num, v.Price, v.UserID, v.SoldAt, v.CanceledAt))
	}

	c.Response().Header().Set("Content-Type", `text/csv; charset=UTF-8`)
	c.Response().Header().Set("Content-Disposition", `attachment; filename="report.csv"`)
	_, err := io.Copy(c.Response(), body)
	return err
}

func resError(c echo.Context, e string, status int) error {
	if e == "" {
		e = "unknown"
	}
	if status < 100 {
		status = 500
	}
	return c.JSON(status, map[string]string{"error": e})
}

func init() {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true&charset=utf8mb4",
		os.Getenv("DB_USER"), os.Getenv("DB_PASS"),
		os.Getenv("DB_HOST"), os.Getenv("DB_PORT"),
		os.Getenv("DB_DATABASE"),
	)

	var err error
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}

	cmd := exec.Command("../../db/init.sh")
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	if err := cmd.Run(); err != nil {
		log.Fatal(err)
	}

	cache, err = rapidash.New(
		// rapidash.LogMode(rapidash.LogModeServerDebug),
		// rapidash.LogEnabled(true),
		rapidash.ServerAddrs([]string{"/tmp/memcached.sock"}),
		rapidash.MaxIdleConnections(4000),
		rapidash.MaxRetryCount(10),
		rapidash.RetryInterval(time.Microsecond*30),
		rapidash.SecondLevelCachePessimisticLock(false),
		rapidash.SecondLevelCacheOptimisticLock(false),
		rapidash.SecondLevelCacheTablePessimisticLock("reservations", true),
	)
	if err != nil {
		log.Fatal(err)
	}
	if err := cache.Flush(); err != nil {
		log.Fatal(err)
	}
	for _, tableStruct := range readOnlyTables {
		if err := cache.WarmUpFirstLevelCache(db, tableStruct); err != nil {
			log.Fatal(err)
		}
	}
	for _, tableStruct := range readWriteTables {
		if err := cache.WarmUpSecondLevelCache(db, tableStruct); err != nil {
			log.Fatal(err)
		}
	}

	if err := warmUp(); err != nil {
		log.Fatal(err)
	}

}

func warmUp() error {
	cacheTx, err := cache.Begin(db)
	if err != nil {
		return err
	}
	defer func() {
		cacheTx.RollbackUnlessCommitted()
	}()
	warmUpUser := func() error {
		rows, err := db.Query("SELECT id, login_name FROM users")
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var user User
			if err := rows.Scan(&user.ID, &user.LoginName); err != nil {
				return err
			}
			if err := cacheTx.FindByQueryBuilder(rapidash.NewQueryBuilder("users").Eq("id", user.ID), &user); err != nil {
				return err
			}
			if err := cacheTx.FindByQueryBuilder(rapidash.NewQueryBuilder("users").Eq("login_name", user.LoginName), &user); err != nil {
				return err
			}
		}
		return nil
	}

	if err := warmUpUser(); err != nil {
		return err
	}

	if err := cacheTx.Commit(); err != nil {
		return err
	}
	return nil
}
