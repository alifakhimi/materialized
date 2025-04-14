module github.com/alifakhimi/materialized/examples

go 1.24.0

replace (
	github.com/alifakhimi/materialized => ../
)

require (
	github.com/alifakhimi/materialized v0.0.0-20250402045309-88ea2810f10c
	gorm.io/driver/sqlite v1.5.7
	gorm.io/gorm v1.25.12
)

require (
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/mattn/go-sqlite3 v1.14.22 // indirect
	github.com/oklog/ulid/v2 v2.1.0 // indirect
	golang.org/x/text v0.14.0 // indirect
)
