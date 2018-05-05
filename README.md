## Python Moat API

This tool is designed to assist in any quick pulls from [Moat's](https://moat.com/) helpful API service. <br>

Initially designed for an older version of Moat, their api added additional requests to handle trended data.<br>

### Initiating Moat Requests

`moat_req = moat.moat_requests("api_key", "password")`

You can add metrics by using the `metric` method

`moat_req.metric("human_and_avoc", "video_exposure_time", "reached_complete_percent")`

Setting the granularity of date can be done using the `date_range` method. Valid ranges include `weeks`, `days`, `months` and `quarters`

`moat_req.date_range("2017-10-01", "2018-04-30", granularity="days")`

After setting metrics, use `build()` to generate the query then pass the request using `send_query()`
