<html>
<body>
<h2>This is our REST service</h2>
<p> An example query: <br>
	<b>QUERY</b>: The top 4 users by id that mentioned at least 1 among Arsenal(34613288), Chelsea(22910295) and Juventus(253508662) in the last 4 months <br>
    <b>URL</b>: http://localhost:8080/rest/rest/search/users/thatMentioned?users=34613288,22910295,253508662&amp;atLeast=1&amp;byId=true&amp;take=4&amp;when=months_ago&amp;back=4</p>
</body>
</html>
