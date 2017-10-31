var conf = {
    _id: "singleNodeRepl",
    members: [
        {
            _id: 0,
            host: "localhost:27017"
        }
    ]
};
rs.initiate(conf);
printjson(rs.conf());
printjson(rs.status());
var status = rs.status();
while(status.members[0].stateStr != 'PRIMARY') {
    printjson('Waiting for active Primary Node.');
    status = rs.status();
}
printjson(rs.status());