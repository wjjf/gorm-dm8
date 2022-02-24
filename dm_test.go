package gorm_dm8

import (
	"fmt"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
	"log"
	"os"
	"testing"
	"time"
)

var db *gorm.DB

type User struct {
	ID                int
	LoginName         string `gorm:"unique;not null;index" json:"login_name"`
	Name              string `json:"name" gorm:"index"`
	EncryptedPassword string `json:"encrypted_password"`
	PasswordSalt      string `json:"password_salt"`

	RoleCode int  `json:"role_code"`
	Actived  bool `json:"actived"`
	Source   int  `json:"source"`

	TokenSince        int       `json:"token_since"`
	LoginFailedTimes  int       `json:"login_failed_times"`
	LastLoginFailedAt time.Time `json:"last_login_failed_at"`
	Phone             string    `json:"phone" gorm:"index"`
	Title             string    `json:"title"`
	MXUserID          int       `json:"mx_user_id"`
	MXDeptID          int       `json:"mx_dept_id"`

	NoAuth bool `json:"no_auth"`
}

func TestDB(t *testing.T) {
	var err error
	dsn := fmt.Sprintf("dm://%s:%s@%s:%s?&appName=%s", "SDP", "1111", "192.168.100.90", "5246", "SDP")
	gormLoggerFile, err := os.Create("./gorm.log")
	if err != nil {
		fmt.Println(err)
		return
	}

	db, err = gorm.Open(Open(dsn), &gorm.Config{
		Logger: gormlogger.New(log.New(gormLoggerFile, "\r\n", log.LstdFlags), gormlogger.Config{
			SlowThreshold: 200 * time.Millisecond,
			LogLevel:      gormlogger.Info,
			Colorful:      true,
		}),
		DisableForeignKeyConstraintWhenMigrating: true,
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	var user User
	db.First(&user, 2)
	user.Name = "weihao"
	db.Save(user)
}
