package clustering

import (
	"reflect"
	"testing"

	"github.com/IBM/operator-for-redis-cluster/internal/testutil"

	"github.com/IBM/operator-for-redis-cluster/pkg/redis"
)

func TestReplacePrimaries(t *testing.T) {
	_, primary1 := testutil.NewRedisPrimaryNode("primary1", "zone1", "pod1", "node1", []string{"1"})
	_, primary2 := testutil.NewRedisPrimaryNode("primary2", "zone2", "pod2", "node2", []string{"2"})
	_, primary3 := testutil.NewRedisPrimaryNode("primary3", "zone3", "pod3", "node3", []string{"3"})
	_, primary4 := testutil.NewRedisPrimaryNode("primary4", "zone1", "pod4", "node1", []string{""})

	type args struct {
		oldPrimaries       redis.Nodes
		newPrimaries       redis.Nodes
		newNodesNoRole     redis.Nodes
		nbPrimary          int32
		nbPrimaryToReplace int32
	}
	tests := []struct {
		name    string
		args    args
		want    redis.Nodes
		want2   redis.Nodes
		wantErr bool
	}{
		{
			name: "empty slices",
			args: args{
				oldPrimaries:       redis.Nodes{},
				newPrimaries:       redis.Nodes{},
				newNodesNoRole:     redis.Nodes{},
				nbPrimary:          0,
				nbPrimaryToReplace: 0,
			},
			want:    redis.Nodes{},
			want2:   redis.Nodes{},
			wantErr: false,
		},
		{
			name: "no primary to replace",
			args: args{
				oldPrimaries:       redis.Nodes{&primary1, &primary2, &primary3},
				newPrimaries:       redis.Nodes{},
				newNodesNoRole:     redis.Nodes{},
				nbPrimary:          3,
				nbPrimaryToReplace: 0,
			},
			want:    redis.Nodes{&primary1, &primary2, &primary3},
			want2:   redis.Nodes{},
			wantErr: false,
		},
		{
			name: "one primary to replace",
			args: args{
				oldPrimaries:       redis.Nodes{&primary1, &primary2, &primary3},
				newPrimaries:       redis.Nodes{},
				newNodesNoRole:     redis.Nodes{&primary4},
				nbPrimary:          3,
				nbPrimaryToReplace: 1,
			},
			want:    redis.Nodes{&primary1, &primary2, &primary4},
			want2:   redis.Nodes{&primary4},
			wantErr: false,
		},
		{
			name: "one primary to replace, current primary already migrated",
			args: args{
				oldPrimaries:       redis.Nodes{&primary1, &primary2},
				newPrimaries:       redis.Nodes{&primary3},
				newNodesNoRole:     redis.Nodes{&primary4},
				nbPrimary:          3,
				nbPrimaryToReplace: 1,
			},
			want:    redis.Nodes{&primary1, &primary3, &primary4},
			want2:   redis.Nodes{&primary4},
			wantErr: false,
		},
		{
			name: "not enough new nodes",
			args: args{
				oldPrimaries:       redis.Nodes{&primary1, &primary2, &primary3},
				newPrimaries:       redis.Nodes{},
				newNodesNoRole:     redis.Nodes{&primary4},
				nbPrimary:          3,
				nbPrimaryToReplace: 2,
			},
			want:    redis.Nodes{&primary1, &primary2, &primary4},
			want2:   redis.Nodes{&primary4},
			wantErr: true,
		},
		{
			name: "not enough primaries",
			args: args{
				oldPrimaries:       redis.Nodes{&primary1, &primary2, &primary3},
				newPrimaries:       redis.Nodes{},
				newNodesNoRole:     redis.Nodes{&primary4},
				nbPrimary:          5,
				nbPrimaryToReplace: 1,
			},
			want:    redis.Nodes{&primary1, &primary2, &primary3, &primary4},
			want2:   redis.Nodes{&primary4},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got2, err := SelectPrimariesToReplace(tt.args.oldPrimaries, tt.args.newPrimaries, tt.args.newNodesNoRole, tt.args.nbPrimary, tt.args.nbPrimaryToReplace)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReplacePrimaries() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			sortedGot := got.SortNodes()
			sortedGot2 := got2.SortNodes()
			sortedWant := tt.want.SortNodes()
			sortedWant2 := tt.want2.SortNodes()
			if !reflect.DeepEqual(sortedGot, sortedWant) {
				t.Errorf("ReplacePrimaries().selectedPrimaries = %v, want %v", sortedGot, sortedWant)
			}
			if !reflect.DeepEqual(sortedGot2, sortedWant2) {
				t.Errorf("ReplacePrimaries().newSelectedPrimaries = %v, want %v", sortedGot2, sortedWant2)
			}
		})
	}
}
