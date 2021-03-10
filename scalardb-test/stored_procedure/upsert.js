function upsert(update, query) {
    var collection = getContext().getCollection();
    //var update = { 'id': "0:1", 'accountId': 0, 'type': 1, 'balance': 99 };
    //var query = 'SELECT * FROM tx_transfer t WHERE t.id = "0:1"';

    function merge(source, update) {
        for (var column in update) {
            if (update[column].constructor == Object) {
                source[column] = merge(source[column], update[column]);
            } else {
                source[column] = update[column];
            }
        }
        return source;
    }

    // Query documents and take 1st item.
    var isAccepted = __.queryDocuments(
        __.getSelfLink(),
        query,
        function (err, records, options) {
            if (err) throw err;

            if (!records || records.length != 1) {
                throw new Error("no docs found.");
            }
            else {
                const upserted = merge(records[0], update);
                const isMutationAccepted = __.upsertDocument(__.getSelfLink(), upserted,
                    (error, result, options) => {
                        if (error) throw error;
                    });
                if (!isMutationAccepted) {
                    throw new Error("The query was not accepted by the server.");
                }
            }
        }
    );

    if (!isAccepted) throw new Error('The query was not accepted by the server.');
}
