# [5.0.0](https://github.com/vmihailenco/taskq/compare/v3.2.9...v5.0.0) (2026-11-13)


### Features

* release as v5 on go-redis v9, require Go 1.25+ ([8c03145](https://github.com/vmihailenco/taskq/commit/8c03145c34ff3ceba28e8d8c46135e053171efd0))


### Bug Fixes

* crash due to nil msg.Ctx. Fixes [#177](https://github.com/vmihailenco/taskq/issues/177) ([c24c993](https://github.com/vmihailenco/taskq/commit/c24c993ad9e1b9c4c3152f27b668a61697422501)), closes [#153](https://github.com/vmihailenco/taskq/issues/153)
* dont automaticaly start consumer ([dde6a3f](https://github.com/vmihailenco/taskq/commit/dde6a3fd8995114b5924092fe0ef7d9a4b7b03f3))


### BREAKING CHANGES

* the module path changes to github.com/vmihailenco/taskq/v5 and
taskq now uses go-redis v9 types in its public API. v5 is the v3 API on top of
go-redis v9: change the import paths, pass a go-redis v9 client, and use Go 1.25
or newer. Custom implementations of taskq.Redis must add EvalRO and EvalShaRO;
custom implementations of redisq.RedisStreamClient must use the go-redis v9
XTrimMaxLen and ZAdd signatures.



## [3.2.9](https://github.com/vmihailenco/taskq/compare/v3.2.8...v3.2.9) (2022-08-24)


### Bug Fixes

* swapped dst and src arguments in zstd decode call ([e61a842](https://github.com/vmihailenco/taskq/commit/e61a84219a8fe65444da5ca9b19571d2245633f2))
* Use localStorage for memqueue tests instead of Redis for [#162](https://github.com/vmihailenco/taskq/issues/162) ([b2ec9f5](https://github.com/vmihailenco/taskq/commit/b2ec9f53b0a3182b49c1c1510172e3ab6ac34b85))


### Features

* allow set backoff duration for redis scheduler ([f6818a8](https://github.com/vmihailenco/taskq/commit/f6818a888f92e6a78e022aae2083d202bfdd3726))



## [3.2.8](https://github.com/vmihailenco/taskq/compare/v3.2.7...v3.2.8) (2021-11-18)


### Bug Fixes

* ack msg before we delete it ([bac023a](https://github.com/vmihailenco/taskq/commit/bac023a71ba191e60f43ce3ca01a25d08d0a70c2))
* adding ctx to msg ([819b42b](https://github.com/vmihailenco/taskq/commit/819b42b66bf482187843670a4a2fc288e9173e29))



## [3.2.7](https://github.com/vmihailenco/taskq/compare/v3.2.6...v3.2.7) (2021-10-28)


### Bug Fixes

* **redisq:** rework tests to use redis client ([670be0f](https://github.com/vmihailenco/taskq/commit/670be0f0ba7ee729df4c6e89c0c571340914f936))
* **redsiq:** call xack inside delete in redsiq ([2f6bd74](https://github.com/vmihailenco/taskq/commit/2f6bd74c006132be6cbec74f9c4808888da34aff))



## [3.2.6](https://github.com/vmihailenco/taskq/compare/v3.2.5...v3.2.6) (2021-10-11)


### Bug Fixes

* introduce interfaces to allow mocking in tests ([6bc8f3b](https://github.com/vmihailenco/taskq/commit/6bc8f3b0462812996c39605c10428b43460696ff))



