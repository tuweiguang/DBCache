package DBCache

import (
	"encoding/json"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis"
	"github.com/jinzhu/gorm"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"testing"
	"time"
)

//test table
type User struct {
	Id        int64     `gorm:"column:id;primary_key"`
	UserName  string    `gorm:"column:username"`
	Secret    string    `gorm:"column:secret;type:varchar(1000)"`
	CreatedAt time.Time `gorm:"column:created_at"`
	UpdatedAt time.Time `gorm:"column:updated_at"`
}

func (m *User) TableName() string {
	return "users"
}

func Init() (dc *DBCache, mk sqlmock.Sqlmock, mysql *gorm.DB, rdb *redis.Client) {
	//1.数据库
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		panic(err)
	}
	mk = mock

	mysql, err = gorm.Open("mysql", db)
	if err != nil {
		panic(err)
	}

	//2.redis
	rds, err := miniredis.Run()
	if err != nil {
		panic(err)
	}

	rdb = redis.NewClient(&redis.Options{
		Addr: rds.Addr(),
	})

	//3.CachesManager
	dc = NewCache(mysql, rdb, zap.NewExample(), "users", WithStatSwitch(false))

	return
}

func TestCachesManager_Exec(t *testing.T) {
	dc, mk, _, _ := Init()
	user := &User{
		UserName:  "Kevin",
		Secret:    "123456",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	mk.ExpectBegin()
	mk.ExpectExec("INSERT INTO `users` (`username`,`secret`,`created_at`,`updated_at`) VALUES (?,?,?,?)").
		WithArgs(user.UserName, user.Secret, user.CreatedAt, user.UpdatedAt).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mk.ExpectCommit()

	key := fmt.Sprintf("user:%v", user.UserName)
	err := dc.Exec(key, func(conn *gorm.DB) error {
		//fail ???
		//sql := "INSERT INTO `users` (`username`,`secret`,`created_at`,`updated_at`) VALUES (?,?,?,?)"
		//result := conn.Debug().Exec(sql, user.UserName, user.Secret, user.CreatedAt, user.UpdatedAt)

		result := conn.Debug().Create(user)
		return result.Error
	})
	assert.Nil(t, err)
}

func TestCachesManager_QueryRow(t *testing.T) {
	dc, mk, _, _ := Init()
	user := &User{
		Id:        1,
		UserName:  "gopher0",
		Secret:    "123456",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	mk.ExpectQuery("SELECT * FROM `users`  WHERE (username = ? AND secret = ?) "+
		"ORDER BY `users`.`id` ASC LIMIT 1").
		WithArgs(user.UserName, user.Secret).
		WillReturnRows(
			// 这里要跟结果集包含的列匹配，因为查询是 SELECT * 所以表的字段都要列出来
			sqlmock.NewRows([]string{"id", "username", "secret", "created_at", "updated_at"}).
				AddRow(user.Id, user.UserName, user.Secret, user.CreatedAt, user.UpdatedAt))

	key := fmt.Sprintf("user:%v", user.UserName)
	u := &User{}
	err := dc.QueryRow(u, key, func(conn *gorm.DB, v interface{}) error {
		//result := conn.Debug().Where("username = ? AND secret = ?", user.UserName, user.Secret).First(u)

		result := conn.Debug().Raw("SELECT * FROM `users`  WHERE (username = ? AND secret = ?) "+
			"ORDER BY `users`.`id` ASC LIMIT 1", user.UserName, user.Secret).Scan(v)
		return result.Error
	}, nil)
	assert.Nil(t, err)

	// test hit cache
	err = dc.QueryRow(u, key, func(conn *gorm.DB, v interface{}) error {
		//result := conn.Debug().Where("username = ? AND secret = ?", user.UserName, user.Secret).First(u)

		result := conn.Debug().Raw("SELECT * FROM `users`  WHERE (username = ? AND secret = ?) "+
			"ORDER BY `users`.`id` ASC LIMIT 1", user.UserName, user.Secret).Scan(v)
		return result.Error
	}, nil)
	assert.Nil(t, err)

	// test not found
	user.UserName = "gopher1"
	key = fmt.Sprintf("user:%v", user.UserName)
	err = dc.QueryRow(u, key, func(conn *gorm.DB, v interface{}) error {
		//result := conn.Debug().Where("username = ? AND secret = ?", user.UserName, user.Secret).First(u)

		result := conn.Debug().Raw("SELECT * FROM `users`  WHERE (username = ? AND secret = ?) "+
			"ORDER BY `users`.`id` ASC LIMIT 1", user.UserName, user.Secret).Scan(v)
		return result.Error
	}, nil)
}

func BenchmarkDBCache_QueryRow_DifferKey(b *testing.B) {
	dc, mk, _, _ := Init()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		user := &User{
			Id:        int64(i),
			UserName:  fmt.Sprintf("benchgopher%v", i),
			Secret:    "123456",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}

		mk.ExpectQuery("SELECT * FROM `users`  WHERE (username = ? AND secret = ?) "+
			"ORDER BY `users`.`id` ASC LIMIT 1").
			WithArgs(user.UserName, user.Secret).
			WillReturnRows(
				// 这里要跟结果集包含的列匹配，因为查询是 SELECT * 所以表的字段都要列出来
				sqlmock.NewRows([]string{"id", "username", "secret", "created_at", "updated_at"}).
					AddRow(user.Id, user.UserName, user.Secret, user.CreatedAt, user.UpdatedAt))

		key := fmt.Sprintf("user:%v", user.UserName)
		u := &User{}
		dc.QueryRow(u, key, func(conn *gorm.DB, v interface{}) error {
			//result := conn.Debug().Where("username = ? AND secret = ?", user.UserName, user.Secret).First(u)

			result := conn.Raw("SELECT * FROM `users`  WHERE (username = ? AND secret = ?) "+
				"ORDER BY `users`.`id` ASC LIMIT 1", user.UserName, user.Secret).Scan(v)
			return result.Error
		}, nil)
	}
	b.StopTimer()
}

func BenchmarkDBCache_OriginalQueryRow_DifferKey(b *testing.B) {
	_, mk, mysql, rdb := Init()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		user := &User{
			Id:        int64(i),
			UserName:  fmt.Sprintf("benchgopher%v", i),
			Secret:    "123456",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}

		mk.ExpectQuery("SELECT * FROM `users`  WHERE (username = ? AND secret = ?) "+
			"ORDER BY `users`.`id` ASC LIMIT 1").
			WithArgs(user.UserName, user.Secret).
			WillReturnRows(
				// 这里要跟结果集包含的列匹配，因为查询是 SELECT * 所以表的字段都要列出来
				sqlmock.NewRows([]string{"id", "username", "secret", "created_at", "updated_at"}).
					AddRow(user.Id, user.UserName, user.Secret, user.CreatedAt, user.UpdatedAt))

		key := fmt.Sprintf("user:%v", user.UserName)
		u := &User{}
		result := mysql.Raw("SELECT * FROM `users`  WHERE (username = ? AND secret = ?) "+
			"ORDER BY `users`.`id` ASC LIMIT 1", user.UserName, user.Secret).Scan(u)
		if result.Error != nil {
			break
		}

		data, err := json.Marshal(u)
		if err != nil {
			break
		}

		err = rdb.Set(key, data, 86400*time.Second).Err()
		if err != nil {
			break
		}
	}
	b.StopTimer()
}

func BenchmarkDBCache_QueryRow_SameKey(b *testing.B) {
	dc, mk, _, _ := Init()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		user := &User{
			Id:        int64(i),
			UserName:  "gopher",
			Secret:    "123456",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}

		mk.ExpectQuery("SELECT * FROM `users`  WHERE (username = ? AND secret = ?) "+
			"ORDER BY `users`.`id` ASC LIMIT 1").
			WithArgs(user.UserName, user.Secret).
			WillReturnRows(
				// 这里要跟结果集包含的列匹配，因为查询是 SELECT * 所以表的字段都要列出来
				sqlmock.NewRows([]string{"id", "username", "secret", "created_at", "updated_at"}).
					AddRow(user.Id, user.UserName, user.Secret, user.CreatedAt, user.UpdatedAt))

		key := fmt.Sprintf("user:%v", user.UserName)
		u := &User{}
		dc.QueryRow(u, key, func(conn *gorm.DB, v interface{}) error {
			//result := conn.Debug().Where("username = ? AND secret = ?", user.UserName, user.Secret).First(u)

			result := conn.Raw("SELECT * FROM `users`  WHERE (username = ? AND secret = ?) "+
				"ORDER BY `users`.`id` ASC LIMIT 1", user.UserName, user.Secret).Scan(v)
			return result.Error
		}, nil)
	}
	b.StopTimer()
}

func BenchmarkDBCache_OriginalQueryRow_SameKey(b *testing.B) {
	_, mk, mysql, rdb := Init()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		user := &User{
			Id:        int64(i),
			UserName:  "gopher",
			Secret:    "123456",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}

		mk.ExpectQuery("SELECT * FROM `users`  WHERE (username = ? AND secret = ?) "+
			"ORDER BY `users`.`id` ASC LIMIT 1").
			WithArgs(user.UserName, user.Secret).
			WillReturnRows(
				// 这里要跟结果集包含的列匹配，因为查询是 SELECT * 所以表的字段都要列出来
				sqlmock.NewRows([]string{"id", "username", "secret", "created_at", "updated_at"}).
					AddRow(user.Id, user.UserName, user.Secret, user.CreatedAt, user.UpdatedAt))

		key := fmt.Sprintf("user:%v", user.UserName)
		u := &User{}
		result := mysql.Raw("SELECT * FROM `users`  WHERE (username = ? AND secret = ?) "+
			"ORDER BY `users`.`id` ASC LIMIT 1", user.UserName, user.Secret).Scan(u)
		if result.Error != nil {
			break
		}

		data, err := json.Marshal(u)
		if err != nil {
			break
		}

		err = rdb.Set(key, data, 86400*time.Second).Err()
		if err != nil {
			break
		}
	}
	b.StopTimer()
}
