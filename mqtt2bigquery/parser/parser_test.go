package parser

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
)

func TestRecord_Simple(t *testing.T) {
	const js = `{"ReceivedTimestamp":"2020-01-06T04:34:47.057492873Z","client":"192.168.8.68:54439","content":"stahtd[16030]: [STA-TRACKER].stahtd_dump_event(): {\"message_type\":\"STA_ASSOC_TRACKER\",\"mac\":\"c8:3c:85:d3:e2:3f\",\"wpa_auth_delta\":\"20000\",\"vap\":\"ath5\",\"assoc_status\":\"0\",\"event_type\":\"soft failure\",\"assoc_delta\":\"0\",\"dns_resp_seen\":\"yes\",\"ip_assign_type\":\"roamed\",\"auth_delta\":\"0\",\"event_id\":\"2\",\"auth_ts\":\"2416527.903485\"}","facility":1,"hostname":"U7PG2,802aa853a30a,v4.0.69.10871:","priority":14,"severity":6,"tag":"","timestamp":"2020-01-05T20:34:47Z","tls_peer":""}`
	ts, _ := time.Parse(time.RFC3339Nano, "2020-01-06T04:34:47.057492873Z")

	msg := make(map[string]interface{})
	if err := json.NewDecoder(strings.NewReader(js)).Decode(&msg); err != nil {
		t.Fatal(err)
	}

	rp := newRecord(3)
	rp.addField(Timestamp{}, 0, "ReceivedTimestamp")
	rp.addField(String{}, 1, "client")
	rp.addField(Integer{}, 2, "facility")

	values, err := rp.ParseAsRecord(msg)
	if err != nil {
		t.Fatal(err)
	}
	if want, got := ts, values[0].(time.Time); want != got {
		t.Errorf("values[0] = %q; want %q", got, want)
	}
	if want, got := "192.168.8.68:54439", values[1].(string); want != got {
		t.Errorf("values[1] = %q; want %q", got, want)
	}
	if want, got := float64(1), values[2].(float64); want != got {
		t.Errorf("values[2] = %f; want %f", got, want)
	}
}

func TestRecord_Nested(t *testing.T) {
	const js = `{"SocketFamily":1,"SocketProtocol":1,"Message":{"Id":10809,"Response":false,"Opcode":0,"Authoritative":false,"Truncated":false,"RecursionDesired":true,"RecursionAvailable":false,"Zero":false,"AuthenticatedData":false,"CheckingDisabled":false,"Rcode":0,"Question":[{"Name":"www.purpleair.com.","Qtype":1,"Qclass":1}],"Answer":null,"Ns":null,"Extra":null},"Timestamp":"2020-02-17T19:52:34.962821944Z"}`

	msg := make(map[string]interface{})
	if err := json.NewDecoder(strings.NewReader(js)).Decode(&msg); err != nil {
		t.Fatal(err)
	}

	rp := newRecord(3)
	rp.addField(Integer{}, 0, "SocketFamily")
	rp.addField(Integer{}, 1, "SocketProtocol")
	sr := newRecord(1)
	rp.addField(sr, 2, "Message")
	sr.addField(Integer{}, 0, "Id")

	values, err := rp.ParseAsRecord(msg)
	if err != nil {
		t.Fatal(err)
	}
	if want, got := float64(1), values[0].(float64); want != got {
		t.Errorf("values[0] = %f; want %f", got, want)
	}
	if want, got := float64(1), values[1].(float64); want != got {
		t.Errorf("values[1] = %f; want %f", got, want)
	}
	if want, got := float64(10809), values[2].([]bigquery.Value)[0].(float64); want != got {
		t.Errorf("values[3] = %f; want %f", got, want)
	}
}
