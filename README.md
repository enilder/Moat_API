## Python Moat API

This tool is designed to assist in any quick pulls from [Moat's](https://moat.com/) helpful API service. <br>

Returns your dataformatted in a pandas dataframe.<br>

### Initializing Moat Requests

Initialize requests by passing your username and password in the `moat_request` function

`moat_req = moat.moat_requests("api_key", "password")`

You can add metrics by using the `metric` method

`moat_req.metric("human_and_avoc", "video_exposure_time", "reached_complete_percent")`

Specify levels using the `level()` method. You can specify multiple levels to return results grouped by levels specified as in a group by SQL clause.

`moat_req.levels("level1", "level2")`

Setting the granularity of date can be done using the `date_range` method. Valid ranges include `weeks`, `days`, `months` and `quarters`

`moat_req.date_range("2017-10-01", "2018-04-30", granularity="days")`

After setting metrics, use `build()` to generate the query then pass the request using `send_query()`
