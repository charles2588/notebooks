import java.io.*;
import java.net.MalformedURLException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
public class HttpClientTest {
	public static void main(String[] args) {

		// NOTE: you must manually construct wml_credentials hash map below
		// using information retrieved from your IBM Cloud Watson Machine Learning Service instance 
		Map<String, String> wml_credentials = new HashMap<String, String>()
		{{
			put("url", "https://ibm-watson-ml.mybluemix.net");
			put("username", "XXXXXX");
			put("password", "XXXXXX");
		}};
		String wml_auth_header = "Basic " +
				Base64.getEncoder().encodeToString((wml_credentials.get("username") + ":" +
					wml_credentials.get("password")).getBytes(StandardCharsets.UTF_8));
		String wml_url = wml_credentials.get("url") + "/v3/identity/token";
		HttpURLConnection tokenConnection = null;
		HttpURLConnection scoringConnection = null;
		BufferedReader tokenBuffer = null;
		BufferedReader scoringBuffer = null;
		try {
			// Getting WML tokenUrl
			URL tokenUrl = new URL(wml_url);
			tokenConnection = (HttpURLConnection) tokenUrl.openConnection();
			tokenConnection.setDoInput(true);
			tokenConnection.setDoOutput(true);
			tokenConnection.setRequestMethod("GET");
			tokenConnection.setRequestProperty("Authorization", wml_auth_header);
			tokenBuffer = new BufferedReader(new InputStreamReader(tokenConnection.getInputStream()));

			StringBuffer jsonString = new StringBuffer();
			String line;
			while ((line = tokenBuffer.readLine()) != null) {
				jsonString.append(line);
			}
			// Scoring request
			URL scoringUrl = new URL("https://ibm-watson-ml.mybluemix.net/v3/wml_instances/85a35f58-e78c-4a4a-9346-48bfd5773af5/published_models/0cb966e9-43b8-468e-b6bd-261bce64fc32/deployments/9af6f026-04ce-48ce-be77-a2a3adf76288/online");
			String wml_token = "Bearer " +
					jsonString.toString()
							.replace("\"","")
							.replace("}", "")
							.split(":")[1];
			scoringConnection = (HttpURLConnection) scoringUrl.openConnection();
			scoringConnection.setDoInput(true);
			scoringConnection.setDoOutput(true);
			scoringConnection.setRequestMethod("POST");
			scoringConnection.setRequestProperty("Accept", "application/json");
			scoringConnection.setRequestProperty("Authorization", wml_token);
			scoringConnection.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
			OutputStreamWriter writer = new OutputStreamWriter(scoringConnection.getOutputStream(), "UTF-8");

			// NOTE: manually define and pass the array(s) of values to be scored in the next line
			String payload = "{\"fields\": [\"GENDER\", \"AGE\", \"MARITAL_STATUS\", \"PROFESSION\"], \"values\": [[\"M\", 27, \"Single\", \"Professional\"],[\"F\", 23, \"Single\", \"Professional\"]]}";
			writer.write(payload);
			writer.close();

			scoringBuffer = new BufferedReader(new InputStreamReader(scoringConnection.getInputStream()));
			StringBuffer jsonStringScoring = new StringBuffer();
			String lineScoring;
			while ((lineScoring = scoringBuffer.readLine()) != null) {
				jsonStringScoring.append(lineScoring);
			}
			System.out.println(jsonStringScoring);
		} catch (IOException e) {
			System.out.println("The URL is not valid.");
			System.out.println(e.getMessage());
		}
		finally {
			if (tokenConnection != null) {
				tokenConnection.disconnect();
			}
			if (tokenBuffer != null) {
				try{
				tokenBuffer.close();
				}catch (IOException e) {
					System.out.println("Could not close tokenBuffer");
					System.out.println(e.getMessage());
				}
			}
			if (scoringConnection != null) {
				scoringConnection.disconnect();
			}
			if (scoringBuffer != null) {
				try{
				scoringBuffer.close();
				}catch (IOException e) {
					System.out.println("Could not close scoringBuffer");
					System.out.println(e.getMessage());
				}
				
			}
		}
	}
}