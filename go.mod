module github.com/ariefdarmawan/flexmy

go 1.16

replace github.com/sebarcode/codekit => D:\Coding\lib\codekit

replace github.com/sebarcode/logger => D:\Coding\lib\logger

replace git.kanosolution.net/kano/dbflex => D:\Coding\lib\dbflex

require (
	git.kanosolution.net/kano/dbflex v1.0.15
	github.com/go-sql-driver/mysql v1.6.0
	github.com/sebarcode/codekit v0.0.0-20220616144406-d7c5cafaca19
	github.com/smartystreets/goconvey v1.6.4
)
