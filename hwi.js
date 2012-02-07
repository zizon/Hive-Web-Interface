var hwi = {
	debug : function(message) {
		if (true && console && console.debug
				&& console.debug instanceof Function) {
			console.debug(message);
		}
	},
	query : function() {
		$("#run").addClass("disabled");
		query_string = $("#query").val();
		$.ajax({
			type : "POST",
			url : "/hwi/submitQuery.jsp",
			data : {
				query : query_string
			},
			success : function(data) {
				hwi.debug(data);
				hwi.message("success",data.message);
				hwi.jobs([{
					"id":data.id,
					"status":"PREP",
					"map":0,
					"reduce":0,
					"query":query_string
				}]);
				$("#run").removeClass("disabled");
			},
			error : function() {
				hwi.message("important","fail to submit query");
				$("#run").removeClass("disabled");
			},
			headers : {"Authorization" : "Basic " + hwi.cookie("basic")},
			dataType: "json"
			});
	},
	delay : function(callback, interval) {
		if (callback && callback instanceof Function) {
			window.setTimeout(callback, interval ? interval : 1000);
			hwi.debug("set callback");
		} else {
			hwi.debug("no callback");
		}
	},
	kill : function(id) {
		$("#" + id).attr("disabled", true);
		$.ajax({
			type : "DELETE",
			url : "/hwi/kill.jsp?id="+id,
			success : function() {
				hwi.message("notice","kill done");
				$("#" + id).attr("disabled", true);
			},
			error : function() {
				hwi.message("important","kill fail");
				$("#" + id).attr("disabled", false);
			},
			headers : {"Authorization" : "Basic " + hwi.cookie("basic")}
		});
	},
	message : function(flag,content, close) {
		  $("#message").replaceWith("<div id='message' style='text-align:center'><span class='label label-"+flag+"'>"+content+"</span></div>");
		  $("#message").slideDown("slow");
	},
	fetch : function(id) {
		$("#" + id).attr("disabled", true);
		$.ajax({
			type : "GET",
			url : "/hwi/getQueryResult.jsp",
			data : {
				id : id
			},
			success : function(data) {
				hwi.debug(data);
				$("#" + id).attr("disabled", false);
				$("#result").show();
				$("#result").replaceWith("<div id='result'><button class='pull-right' id='close'>x</button><pre>" + $.trim(data) + "</pre></div>");
				$("#close").click(function(){
					$('#result').hide();
				});
			},
			error : function() {
				hwi.message("important","fetch fail");
				$("#" + id).attr("disabled", false);
			},
			dataType : "text",
			headers : {
				"Authorization" : "Basic " + hwi.cookie("basic")
			}
		});
	},
	cache_cookie : {},
	load_cookie : function(){
		if(document.cookie && document.cookie.length > 0){
			 var cookies = document.cookie.split(";");
			 var pair;
			 var position;
			 for (index in cookies) {
				pair = cookies[index];
				position=pair.indexOf("=");
				hwi.cache_cookie[$.trim(pair.substring(0,position))] = $.trim(pair.substring(position+1));
			}
		}
	},
	save_cookie : function(key,value){
		hwi.load_cookie();
		hwi.cache_cookie[key]=value;
		for(key in hwi.cache_cookie){
			document.cookie = key+"="+hwi.cache_cookie[key]+"";
		}
	},
	cookie : function(key) {
		expected = hwi.cache_cookie[key];
		if(expected){
			return expected;
		}else {
			hwi.load_cookie();
		}

		return expected = hwi.cache_cookie[key];
	},
	historys : {},
	jobs : function(data) {
		 for (index in data) {
			 var html ="";
			 var change=false;
			 var row = data[index];
			 if(row.id == "null"){
				 continue;
			 }

			 if(row.id in hwi.historys){
				 if(row.map != hwi.historys[row.id].map || row.reduce != hwi.historys[row.id].reduce){
					 hwi.debug("proress chaned");
					 $("#"+row.id+"-progress").replaceWith("<td id='"+row.id+"-progress'>"+(row.map* 100 + row.reduce * 100)/2+"%</td>");
					 change=true;
				 }
				 if(row.status != hwi.historys[row.id].status){
					 hwi.debug("status changed");
					 $("#"+row.id+"-status").replaceWith("<td id='"+row.id+"-status'>"+row.status+"</td>");
					 change=true;
				 }

				 if(change == true){
					 if(row.status == "SUCCEEDED"){
						 $("#"+row.id).replaceWith("<button id='"+row.id + "' onclick='hwi.fetch(\""+row.id+"\")' class='btn'>fetch</button>");
					 }else{
						 $("#"+row.id).replaceWith("<button id='"+row.id + "' onclick='hwi.kill(\""+row.id+"\")' class='btn'>kill</button>");
					 }

				 }
				 hwi.historys[row.id]=row;
				 continue;
			 }else{
				 hwi.historys[row.id]=row;
				 html += "<tr id='row-"+row.id+"'>";
				 html += "<td>" + row.id + "</td>";
				 html += "<td id='"+row.id+"-status'>" + row.status + "</td>";
				 html += "<td id='"+row.id+"-progress'>" + (row.map * 100 + row.reduce * 100) / 2 + "%</td>";
				 html += "<td>" + row.query + "</td>";
				 html += "<td>";
				 if (row.status == "SUCCEEDED") {
					 html += "<button id='" + row.id
						 + "' onclick='hwi.fetch(\"" + row.id
						 + "\")' class='btn'>fetch</button>";
				 } else {
					 html += "<button id='" + row.id
						 + "' onclick='hwi.kill(\"" + row.id
						 + "\")' class='btn'>kill</button>";
				 }
				 html += "</td>";
				 html += "</tr>";
				 $("#jobs-body").append(html);
				 continue;
			 }
		 }
	},
	_mapping : "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/",
	encode : function(raw){
		var result = "";
		var tracker = 0;
		while(tracker + 3 <= raw.length){
			result += hwi._mapping[ (raw.charCodeAt(tracker) & 0xff) >> 2 ];
			result += hwi._mapping[ ((raw.charCodeAt(tracker) & 0x03) << 4) | ((raw.charCodeAt(tracker+1) & 0xf0) >> 4) ];
			result += hwi._mapping[ ((raw.charCodeAt(tracker+1) & 0x0f) << 2) | ((raw.charCodeAt(tracker+2) & 0xc0) >> 6)];
			result += hwi._mapping[(raw.charCodeAt(tracker+2) & 0x3f)];
			tracker+=3;
		}

		if(tracker < raw.length){
			switch(raw.length - tracker) {
				case 1:
					result += hwi._mapping[ raw.charCodeAt(tracker) >> 2 ];
					result += hwi._mapping[ ((raw.charCodeAt(tracker) & 0x03) << 4) ];
					result += "==";
					break;
				case 2:
					result += hwi._mapping[ raw.cahrCodeAt(tracker)] >> 2;
					result += hwi._mapping[ ((raw.cahrCodeAt(tracker) & 0x03) << 4) |  ((raw.charCodeAt(tracker+1) & 0xf0) >> 4) ];
					result += hwi._mapping[((raw.charCodeAt(tracker+1) & 0x0f) << 2) ];
					result += "=";
					break;
			}
		}
		return result;
	},
	authing : true,
	auth : function(user,password){
			hwi.save_cookie("user",user);
			hwi.save_cookie("basic",hwi.encode(user+":"+password));
			hwi.authing = true;	
	},
	attatch : function() {
		(self = function() {
			$.ajax({
				type : "GET",
				url : "/hwi/getUserQuerys.jsp",
				data : {
					user : hwi.cookie("user")
				},
				success : function(data) {
					if(hwi.authing){
						hwi.message("info","authorization OK");
						hwi.authing=false;
						$("#go").attr("disabled",false);
						login.hide();
						main.show();
					}
					hwi.debug("refresh querys done")
					hwi.jobs(data["querys"])
					hwi.delay(self, 2000)
				},
				error : function() {
					hwi.debug("refresh querys fail")
					hwi.delay(self, 2000)
				},
				headers : {
					"Authorization" : "Basic " + hwi.cookie("basic")
				},
				statusCode : {
					401 : function(){
						var change = false;
						if(hwi.authing){
							hwi.message("important","fail to auth user");
							$("#go").attr("disabled",false);
							change=true;
						}

						if(change){
							hwi.message("important","require auth");
							login.show();
							main.hide();
						}
					}
				}
			});
		})();
		
		var login = $("#login");
		var main = $("#main");
		
		var user = $("#username");
		var password = $("#password");
		var go = $("#go");
		go.click(function(){
			hwi.auth(user.val(),password.val());
			go.attr("disabled",true);
			hwi.message("notice","Authoring user...");
			hwi.authing=true;
		});

		$("#run").click(hwi.query);
		var div_id_usage=$("#usage");
		div_id_usage.hide();

		var span_id_guide_toggle = $("#guide-toggle");
		span_id_guide_toggle.hide();

		var div_id_guide = $("#guide");

		var div_id_guide_click = function(){
			div_id_guide.unbind("click");
			span_id_guide_toggle.hide();
			div_id_usage.slideDown("fast");
			span_id_guide_toggle.attr("guide-status","expand");
		};

		var strong_id_guide_dismiss = $("#guide-dismiss");
		strong_id_guide_dismiss.click(function(){
					div_id_guide.unbind("click");
					span_id_guide_toggle.hide();
					div_id_usage.slideUp("fast");
					span_id_guide_toggle.attr("guide-status","fold");
				});

		div_id_guide.hover(
			function(){
				div_id_guide.bind("click",div_id_guide_click);
				if( span_id_guide_toggle.attr("guide-status") != "expand"){
					span_id_guide_toggle.show();
				}
				div_id_guide.bind("mousemove",function(e){
					span_id_guide_toggle.offset({
						left:16+e.pageX,
						top:e.pageY
					});
				});
			},
			function(){
					span_id_guide_toggle.hide();
					div_id_guide.unbind("mousemove");
					div_id_guide.unbind("click");
			}
		);
	}
};
