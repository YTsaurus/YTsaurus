#include <yt/cpp/mapreduce/examples/tutorial/protobuf_complex_types/data.pb.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/config.h>

#include <util/stream/output.h>
#include <util/system/user.h>

using namespace NYT;

// Редьюсер агрегирует информацию о ссылках на документ с данным заголовком.
class AggregateLinksReducer
    : public IReducer<TTableReader<TLinkEntry>, TTableWriter<TDoc>>
{
public:
    void Do(TReader* reader, TWriter* writer) override
    {
        TDoc doc;
        for (auto& cursor : *reader) {
            auto entry = cursor.MoveRow();
            if (!doc.has_title()) {
                doc.set_title(entry.doc_title());
            }
            doc.add_links()->Swap(entry.mutable_link());
            doc.add_occurrence_count(entry.occurrence_count());
            auto newCount = doc.extra_info().total_occurrence_count() + entry.occurrence_count();
            doc.mutable_extra_info()->set_total_occurrence_count(newCount);
        }
        writer->AddRow(doc);
    }
};
REGISTER_REDUCER(AggregateLinksReducer)

int main() {
    NYT::Initialize();

    TString cluster = "freud";
    auto client = CreateClient(cluster);

    const TString sortedLinksTable  = "//home/tutorial/links_sorted_schematized";

    Cout << "Sorted links table: https://yt.yandex-team.ru/" << cluster << "/#page=navigation&offsetMode=row&path=" << sortedLinksTable << Endl;

    const TString outputTable = "//tmp/" + GetUsername() + "-tutorial-docs-protobuf";

    // Обратите внимание на опцию `InferOutputSchema`,
    // она заставляет навешивать на выходную таблицу схему.
    client->Reduce(
        TReduceOperationSpec()
            .AddInput<TLinkEntry>(sortedLinksTable)
            .AddOutput<TDoc>(outputTable)
            .ReduceBy({"DocTitle"}),
        new AggregateLinksReducer,
        TOperationOptions()
            .InferOutputSchema(true));

    Cout << "Output table: https://yt.yandex-team.ru/" << cluster << "/#page=navigation&offsetMode=row&path=" << outputTable << Endl;

    return 0;
}
