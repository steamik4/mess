package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"golang.org/x/crypto/bcrypt"
	_ "modernc.org/sqlite"
)

var db *sql.DB
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type Client struct {
	conn     *websocket.Conn
	username string
	nickname string
	avatar   string
}

type ClientManager struct {
	clients    map[string]*Client
	register   chan *Client
	unregister chan *Client
	mu         sync.Mutex
}

var manager = ClientManager{
	clients:    make(map[string]*Client),
	register:   make(chan *Client),
	unregister: make(chan *Client),
}

type IncomingMessage struct {
	Action    string `json:"action"`
	Nickname  string `json:"nickname"`
	Username  string `json:"username"`
	Password  string `json:"password"`
	Avatar    string `json:"avatar"`
	Content   string `json:"content"`
	Recipient string `json:"recipient"`
	Query     string `json:"query"`
}

func initDB() {
	var err error
	db, err = sql.Open("sqlite", "./chat.db")
	if err != nil {
		log.Fatal("Ошибка открытия БД: ", err)
	}

	createUsersTable := `CREATE TABLE IF NOT EXISTS users (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		nickname TEXT,
		username TEXT UNIQUE,
		phone TEXT UNIQUE,
		password TEXT,
		avatar TEXT DEFAULT '',
		last_seen TEXT DEFAULT '',
		last_name_change DATETIME
	);`

	createMessagesTable := `CREATE TABLE IF NOT EXISTS messages (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		sender TEXT,
		recipient TEXT,
		content TEXT,
		created_at TEXT
	);`

	_, err = db.Exec(createUsersTable)
	if err != nil {
		log.Fatal("Ошибка создания таблицы пользователей: ", err)
	}

	_, err = db.Exec(createMessagesTable)
	if err != nil {
		log.Fatal("Ошибка создания таблицы сообщений: ", err)
	}

	log.Println("База данных SQLite успешно настроена.")
}

func homePage(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("Telegram-like Messenger Server is running!"))
}

func handleConnections(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Ошибка апгрейда:", err)
		return
	}

	var currentClient *Client

	defer func() {
		if currentClient != nil {
			nowStr := time.Now().Format("02.01 в 15:04")
			db.Exec("UPDATE users SET last_seen = ? WHERE username = ?", nowStr, currentClient.username)

			manager.mu.Lock()
			delete(manager.clients, currentClient.username)
			manager.mu.Unlock()
			log.Printf("Клиент %s отключился\n", currentClient.username)
		}
		ws.Close()
	}()

	for {
		_, msgBytes, err := ws.ReadMessage()
		if err != nil {
			break
		}

		var msg IncomingMessage
		err = json.Unmarshal(msgBytes, &msg)
		if err != nil {
			ws.WriteJSON(map[string]interface{}{"status": "error", "message": "Неверный формат JSON"})
			continue
		}

		switch msg.Action {
		case "register":
			if len(msg.Username) < 4 || len(msg.Password) < 4 {
				ws.WriteJSON(map[string]interface{}{"status": "error", "message": "Логин и пароль должны быть от 4 символов"})
				continue
			}

			hashedPassword, err := bcrypt.GenerateFromPassword([]byte(msg.Password), bcrypt.DefaultCost)
			if err != nil {
				ws.WriteJSON(map[string]interface{}{"status": "error", "message": "Ошибка хэширования"})
				continue
			}

			_, err = db.Exec("INSERT INTO users (nickname, username, phone, password, avatar, last_name_change) VALUES (?, ?, ?, ?, ?, ?)",
				msg.Nickname, msg.Username, msg.Username, string(hashedPassword), msg.Avatar, time.Now())

			if err != nil {
				ws.WriteJSON(map[string]interface{}{"status": "error", "message": "Пользователь с таким юзернеймом уже существует"})
				continue
			}

			ws.WriteJSON(map[string]interface{}{"status": "success", "message": "Регистрация успешна!"})

		case "login":
			var id int
			var storedHash, nickname, avatar string
			err := db.QueryRow("SELECT id, nickname, password, avatar FROM users WHERE username = ?", msg.Username).Scan(&id, &nickname, &storedHash, &avatar)

			if err != nil {
				ws.WriteJSON(map[string]interface{}{"status": "error", "message": "Пользователь не найден"})
				continue
			}

			err = bcrypt.CompareHashAndPassword([]byte(storedHash), []byte(msg.Password))
			if err != nil {
				ws.WriteJSON(map[string]interface{}{"status": "error", "message": "Неверный пароль"})
				continue
			}

			currentClient = &Client{
				conn:     ws,
				username: msg.Username,
				nickname: nickname,
				avatar:   avatar,
			}

			manager.mu.Lock()
			manager.clients[msg.Username] = currentClient
			manager.mu.Unlock()

			ws.WriteJSON(map[string]interface{}{
				"status":   "success",
				"message":  "Авторизация успешна!",
				"nickname": nickname,
				"username": msg.Username,
				"avatar":   avatar,
			})

		case "update_profile":
			if currentClient == nil {
				continue
			}

			var lastNameChange time.Time
			db.QueryRow("SELECT last_name_change FROM users WHERE username = ?", currentClient.username).Scan(&lastNameChange)

			if time.Since(lastNameChange) < 1*time.Minute {
				ws.WriteJSON(map[string]interface{}{
					"status":  "error",
					"message": "Имя можно менять только раз в 1 минуту!",
				})
				continue
			}

			_, err = db.Exec("UPDATE users SET nickname = ?, avatar = ?, last_name_change = ? WHERE username = ?",
				msg.Nickname, msg.Avatar, time.Now(), currentClient.username)
			if err != nil {
				ws.WriteJSON(map[string]interface{}{"status": "error", "message": "Ошибка обновления профиля"})
				continue
			}

			currentClient.nickname = msg.Nickname
			currentClient.avatar = msg.Avatar

			ws.WriteJSON(map[string]interface{}{
				"status":   "success",
				"message":  "Профиль успешно обновлен!",
				"nickname": msg.Nickname,
				"avatar":   msg.Avatar,
			})

		case "search":
			query := msg.Query
			if len(query) > 0 && query[0] == '@' {
				query = query[1:]
			}

			// Поиск строго по уникальному username
			rows, err := db.Query("SELECT nickname, username, avatar, last_seen FROM users WHERE username LIKE ? AND username != ?",
				"%"+query+"%", currentClient.username)
			if err != nil {
				continue
			}

			type UserInfo struct {
				Nickname string `json:"nickname"`
				Username string `json:"username"`
				Avatar   string `json:"avatar"`
				LastSeen string `json:"last_seen"`
			}
			var users []UserInfo
			for rows.Next() {
				var u UserInfo
				rows.Scan(&u.Nickname, &u.Username, &u.Avatar, &u.LastSeen)
				users = append(users, u)
			}

			if err = rows.Err(); err != nil {
				log.Println("Ошибка итерации по результатам поиска:", err)
			}
			rows.Close()

			resp, _ := json.Marshal(map[string]interface{}{
				"type":  "search_results",
				"users": users,
			})
			ws.WriteMessage(websocket.TextMessage, resp)

		case "get_history":
			var rows *sql.Rows
			if msg.Recipient == "" {
				rows, err = db.Query("SELECT sender, recipient, content, created_at FROM messages WHERE recipient = '' ORDER BY id DESC LIMIT 50")
			} else {
				rows, err = db.Query("SELECT sender, recipient, content, created_at FROM messages WHERE (sender = ? AND recipient = ?) OR (sender = ? AND recipient = ?) ORDER BY id ASC",
					currentClient.username, msg.Recipient, msg.Recipient, currentClient.username)
			}

			if err == nil {
				type HistoryMsg struct {
					Sender    string `json:"sender"`
					Recipient string `json:"recipient"`
					Content   string `json:"content"`
					CreatedAt string `json:"created_at"`
				}
				var history []HistoryMsg
				for rows.Next() {
					var hm HistoryMsg
					rows.Scan(&hm.Sender, &hm.Recipient, &hm.Content, &hm.CreatedAt)
					history = append(history, hm)
				}

				if err = rows.Err(); err != nil {
					log.Println("Ошибка итерации по истории сообщений:", err)
				}

				rows.Close()

				if msg.Recipient == "" {
					for i, j := 0, len(history)-1; i < j; i, j = i+1, j-1 {
						history[i], history[j] = history[j], history[i]
					}
				}

				historyJSON, _ := json.Marshal(map[string]interface{}{
					"type":     "history",
					"messages": history,
				})
				ws.WriteMessage(websocket.TextMessage, historyJSON)
			}

		case "message":
			if currentClient == nil {
				continue
			}

			createdAt := time.Now().Format("15:04")

			db.Exec("INSERT INTO messages (sender, recipient, content, created_at) VALUES (?, ?, ?, ?)",
				currentClient.username, msg.Recipient, msg.Content, createdAt)

			fullMessage, _ := json.Marshal(map[string]interface{}{
				"type":        "chat_message",
				"sender":      currentClient.username,
				"sender_nick": currentClient.nickname,
				"sender_av":   currentClient.avatar,
				"recipient":   msg.Recipient,
				"content":     msg.Content,
				"created_at":  createdAt,
			})

			if msg.Recipient == "" {
				manager.mu.Lock()
				for _, client := range manager.clients {
					client.conn.WriteMessage(websocket.TextMessage, fullMessage)
				}
				manager.mu.Unlock()
			} else {
				manager.mu.Lock()
				if recipientClient, ok := manager.clients[msg.Recipient]; ok {
					recipientClient.conn.WriteMessage(websocket.TextMessage, fullMessage)
				}
				if senderClient, ok := manager.clients[currentClient.username]; ok {
					senderClient.conn.WriteMessage(websocket.TextMessage, fullMessage)
				}
				manager.mu.Unlock()
			}
		}
	}
}

func main() {
	initDB()
	defer db.Close()

	http.HandleFunc("/", homePage)
	http.HandleFunc("/ws", handleConnections)

	port := ":8080"
	log.Printf("Сервер запущен на http://localhost%s\n", port)
	http.ListenAndServe(port, nil)
}
