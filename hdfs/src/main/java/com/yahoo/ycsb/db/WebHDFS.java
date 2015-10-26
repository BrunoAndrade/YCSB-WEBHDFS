package com.yahoo.ycsb.db;

import java.io.File;
import java.io.IOException;

import org.apache.http.HttpResponse;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.rits.cloning.Cloner;

/*
 * TODO 
 * 		- OPEN			(GET)		-	Done: review (not JSON response)
 * 		- CREATE		(PUT)		-	Done: review (not JSON response)
 * 		- MKDIR			(PUT)		-	Done: ok (with JSON)
 * 		- DELETE		(DELETE)	-	Done: ok (with JSON)
 * 		- LISTSTATUS	(GET)		-	Done: ok (with JSON)
 * 		- GETFILESTATUS (GET)		-	Done: ok (with JSON)
 * 		- RENAME 		(PUT)		- 	Done: review (with JSON)
 * 		- APPEND		(POST)		-	Done: ok (not JSON response)
 * 
 * */


public class WebHDFS {
	private String SEMI_URL;
	private String USER;
	private CloseableHttpClient HTTP;

	public WebHDFS(String host, int port, String user, Boolean ssl){
		this.USER = user;
		HTTP = HttpClients.createDefault();
		if(ssl)
			this.SEMI_URL = "https://" + host + ":" + port + "/webhdfs/v1/";
		else
			this.SEMI_URL = "http://" + host + ":" + port + "/webhdfs/v1/";
	}
	
	// GET Operations
	public boolean open(String pathHdfs, String user){
		String url = SEMI_URL + pathHdfs + "?user.name=" + user + "&op=OPEN";
		
		try {
		HttpGet get = new HttpGet(url);
		
			ResponseHandler<Boolean> responseHandler = new ResponseHandler<Boolean>() {
				public Boolean handleResponse(final HttpResponse response) throws IOException {
					Boolean ret = false;
					int status = response.getStatusLine().getStatusCode();
					
					if(status == 200){
						HttpEntity entity = response.getEntity();
						if(entity != null){
							byte[] data = EntityUtils.toByteArray(entity);
							ret = true;
						}
					}
					else if(status == 404){
						System.out.println("Error: Open not possible. File not found. Return - " + status + ".\n");
						ret = false;
					}
					else {
						System.out.println("Error: Open not possible. Return - " + status + ".\n");
						ret = false;
					}
					return ret;
				}
			};
			
			return HTTP.execute(get, responseHandler);
		}
		catch(ClientProtocolException e){
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		
		/*
		Response response = Request.Get(url).execute();
		Response response_clone = Request.Get(url).execute();

		HttpResponse http_resp = response_clone.returnResponse();

		if(http_resp.getStatusLine().getStatusCode() == 200){
			long startTime = System.nanoTime();
			byte[] content = response.returnContent().asBytes();
			long endTime = System.nanoTime();
			System.out.println("Success: Opened. Content-Lenght: aprox. " 
								+ (new Double(http_resp.getLastHeader("Content-Length").getValue()) / 1024 / 1024)
								+ "MB | Processing content duration: " + ((endTime-startTime)/1000000000.0) + " seconds.\n");
			return true;
		}
		else if(http_resp.getStatusLine().getStatusCode() == 404){
			System.out.println("Error: Open not possible. File not found. Return - " + http_resp.getStatusLine() + ".\n");
			return false;
		}
		else {
			System.out.println("Error: Open not possible. Return - " + http_resp.getStatusLine() + ".\n");
			return false;
		}*/
	}

	public JSONArray listStatus(String pathHdfs){
		String url = SEMI_URL + pathHdfs + "?user.name=" + USER + "&op=LISTSTATUS";
		
		try {
			Response response = Request.Get(url).execute();
			Response response_clone = new Cloner().deepClone(response);
			
			StatusLine status_line = response_clone.returnResponse().getStatusLine();
			
			if(status_line.getStatusCode() == 200){
				System.out.println("ini");
				JSONObject json_content = (JSONObject) new JSONParser().parse(response.returnContent().asString());
				
				JSONArray list = (JSONArray) ((JSONObject) json_content.get("FileStatuses")).get("FileStatus");
				
				/*
				JSONObject j = null;
				for(int i = 0; i < array.size(); i++){
					
					j = (JSONObject) array.get(i);
					if(((String) j.get("type")).equals(new String("DIRECTORY"))){
						listStatus((String) j.get("pathSuffix"),"hadoop");
					}
					System.out.println(j);
				}
				*/
				return list;
				
			}
			else if(status_line.getStatusCode() == 404){
				System.out.println("Error: ListStatus not possible. File/Directory not found. Return - " + status_line + ".\n");
				JSONArray r = new JSONArray();
				r.add(new Integer(404));
				return r;
			}
			else{
				System.out.println("Error: ListStatus not possible. Return - " + status_line + ".\n");
				return null;
			}
			
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
	public boolean getFileStatus(String pathHdfs){
		String url = SEMI_URL + pathHdfs + "?user.name=" + USER + "&op=GETFILESTATUS";
		
		try {
			Response response = Request.Get(url).execute();
			Response response_clone = new Cloner().deepClone(response);
			
			StatusLine status_line = response_clone.returnResponse().getStatusLine();
			
			if(status_line.getStatusCode() == 200){
				JSONObject json = (JSONObject) new JSONParser().parse(response.returnContent().asString());
				
				System.out.println(json.toJSONString());
				
				return true;
			}
			else if(status_line.getStatusCode() == 404){
				System.out.println("Error: GetFileStatus not possible. File/Director not found. Return - " + status_line + ".\n");
				return false;
			}
			else{
				System.out.println("Error: GetFileStatus not possible. Return - " + status_line + ".\n");
				return false;
			}
			
		} catch (ParseException e) {
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}	

	// PUT Operations
	public boolean mkdir(String pathHdfs){
		String url = SEMI_URL + pathHdfs + "?user.name=" + USER + "&op=MKDIRS";
		
		try {
			
			Response response = Request.Put(url).execute();
			Response response_clone = new Cloner().deepClone(response);
			
			StatusLine status_line = response_clone.returnResponse().getStatusLine();
			
			if(status_line.getStatusCode() == 200){
				JSONObject json = (JSONObject) new JSONParser().parse(response.returnContent().asString());
				
				if((Boolean) json.get("boolean")){
					System.out.println("Success: Mkdir. Return (Response) - " + status_line + " | (Content) - " + json.toJSONString() + ".\n");
					return true;
				}
				else{
					System.out.println("Error: Mkdir not possible. Return (Response) - " + status_line + " | (Content) - " + json.toJSONString() + ".\n");
					return false;
				}
			}
			else{
				System.out.println("Error: Mkdir not possible. Resturn - " + status_line + ".\n");
				return false;
			}
		} catch (ParseException e) {
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}
	public boolean rename(String pathOri, String pathDest){
		String url = SEMI_URL + pathOri + "?user.name=" + USER + "&op=RENAME&destination=/" + pathDest;
		
		try{
			Response response = Request.Put(url).execute();
			Response response_clone = new Cloner().deepClone(response);
			
			StatusLine status_line = response_clone.returnResponse().getStatusLine();
			
			if(status_line.getStatusCode() == 200){
				JSONObject json = (JSONObject) new JSONParser().parse(response.returnContent().asString());
				
				if((Boolean) json.get("boolean")){
					System.out.println("Success: Renamed. Return (Response) - " + status_line + " | (Content) - " + json.toJSONString() + ".\n");
					return true;
				}
				else{
					System.out.println("Error: Rename not possible. Return (Response) - " + status_line + " | (Content) - " + json.toJSONString() + ".\n");
					return false;
				}
			}
			else {
				System.out.println("Error: Rename not possible. Return - " + status_line + ".\n");
				return false;
			}
		} catch (ParseException e) {
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}
	public boolean createCopyFromLocal(String pathHdfs, byte[] data, String overwrite){
		String url = SEMI_URL + pathHdfs + "?user.name=" + USER + "&op=CREATE&overwrite=" + overwrite;
		
		try {
			HttpResponse redirect =  Request.Put(url).execute().returnResponse();
			
			// 307 is status code to temporary_redirect
			if(redirect.getStatusLine().getStatusCode()==307){
				StatusLine status_line = Request.Put(redirect.getLastHeader("Location").getValue())
						.bodyByteArray(data)
						.execute().returnResponse().getStatusLine();
				
				if(status_line.getStatusCode()==201){
					System.out.println("Success: Created. Return - " + status_line.toString() + ".\n");
					return true;
				}
				else{
					System.out.println("Error: Create not possible. Return - " + status_line.toString() + ".\n");
					return false;
				}
			}
			else {
				System.out.println("Error: Create not possible. Return - " + redirect.getStatusLine().toString() + ".\n");
				return false;
			}
		} catch (IOException e) {
			System.out.println("Error: Maybe local file not exist. Verify.");
			return false;
		}
	}
	
	// DELETE Operations
	public boolean delete(String pathHdfs, String recursive){
		String url = SEMI_URL + pathHdfs + "?user.name=" + USER + "&op=DELETE&recursive=" + recursive;
		
		try {
			Response response = Request.Delete(url).execute();
			
			Cloner cloner = new Cloner();
			Response response_clone = cloner.deepClone(response);
			
			StatusLine status_line = response_clone.returnResponse().getStatusLine();
			
			if(status_line.getStatusCode() == 200){
				JSONObject json = (JSONObject) new JSONParser().parse(response.returnContent().asString());
				
				if((Boolean) json.get("boolean")){
					System.out.println("Success: Deleted. Return (Response) - " + status_line + " | (Content) - " + json.toJSONString() + ".\n");
					return true;
				}
				else{
					System.out.println("Error: Delete not possible. Return (Response) - " + status_line + " | (Content) - " + json.toJSONString() + ".\n");
					return false;
				}
			}
			else {
				System.out.println("Error: Delete not Possible. Return - " + status_line + ".\n");
				return false;
			}
			
		} catch (ParseException e) {
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}
	
	// POST Operations
	public boolean appendFromLocalFile(String pathHdfs, String pathLocal){
		String url = SEMI_URL + pathHdfs + "?user.name=" + USER + "&op=APPEND";
		
		try {
			HttpResponse redirect = Request.Post(url).execute().returnResponse();
			
			if(redirect.getStatusLine().getStatusCode() != 404){
				StatusLine status_line = Request.Post(redirect.getLastHeader("Location").getValue())
						.bodyFile(new File(pathLocal), ContentType.APPLICATION_OCTET_STREAM)
						.execute().returnResponse().getStatusLine();

				if(status_line.getStatusCode() == 200){
					System.out.println("Success: Appended. Return - " + status_line + ".\n");
					return true;
				}
				else {
					System.out.println("Error: Append not possible. Return " + status_line + ".\n");
					return false;
				}
			}
			else {
				System.out.println("Error: Append not possible. Maybe file dont exist. Return - " + redirect.getStatusLine() + ".\n");
				return false;
			}
			
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}
}
