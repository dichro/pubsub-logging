package parser

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestSyslog(t *testing.T) {
	const js = `{"ReceivedTimestamp":"2020-01-06T04:34:47.057492873Z","client":"192.168.8.68:54439","content":"stahtd[16030]: [STA-TRACKER].stahtd_dump_event(): {\"message_type\":\"STA_ASSOC_TRACKER\",\"mac\":\"c8:3c:85:d3:e2:3f\",\"wpa_auth_delta\":\"20000\",\"vap\":\"ath5\",\"assoc_status\":\"0\",\"event_type\":\"soft failure\",\"assoc_delta\":\"0\",\"dns_resp_seen\":\"yes\",\"ip_assign_type\":\"roamed\",\"auth_delta\":\"0\",\"event_id\":\"2\",\"auth_ts\":\"2416527.903485\"}","facility":1,"hostname":"U7PG2,802aa853a30a,v4.0.69.10871:","priority":14,"severity":6,"tag":"","timestamp":"2020-01-05T20:34:47Z","tls_peer":""}`
	ts, _ := time.Parse(time.RFC3339Nano, "2020-01-06T04:34:47.057492873Z")

	msg := make(map[string]interface{})
	if err := json.NewDecoder(strings.NewReader(js)).Decode(&msg); err != nil {
		t.Fatal(err)
	}

	rp := newRecord()
	rp.addField(Timestamp{}, "ReceivedTimestamp")
	rp.addField(String{}, "client")
	rp.addField(Integer{}, "facility")

	values, err := rp.ParseRecord(msg)
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
