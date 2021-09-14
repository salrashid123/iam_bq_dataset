package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"

	"golang.org/x/net/http2"
	"golang.org/x/time/rate"
	"google.golang.org/api/cloudresourcemanager/v1"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iam/v1"
)

type Roles struct {
	Roles []Role `json:"roles"`
}

type Role struct {
	Name                string   `json:"name"`
	Role                iam.Role `json:"role"`
	IncludedPermissions []string `json:"included_permissions"`
}

type Permissions struct {
	Permissions []Permission `json:"permissions"`
}

type Permission struct {
	//Permission iam.Permission // there's no direct way to query a given permission detail!
	Name  string   `json:"name"`
	Roles []string `json:"roles"`
}

const (
	maxRequestsPerSecond float64 = 4 // "golang.org/x/time/rate" limiter to throttle operations
	burst                int     = 4
	roleTableName                = "roles"
	permissionsTableName         = "permissions"
)

var (
	cmutex = &sync.Mutex{}
	pmutex = &sync.Mutex{}

	organization = flag.String("organization", "", "OrganizationID")

	mode        = flag.String("mode", "default", "Interation mode: organization|project|default")
	projects    = make([]*cloudresourcemanager.Project, 0)
	bqDataset   = flag.String("bqDataset", os.Getenv("BQ_DATASET"), "BigQuery Dataset to write to")
	bqProjectID = flag.String("bqProjectID", os.Getenv("BQ_PROJECTID"), "Project for the BigQuery Dataset to write to")
	permissions = &Permissions{}
	roles       = &Roles{}
	limiter     *rate.Limiter
	ors         *iam.RolesService

	rolesSchema = bigquery.Schema{
		{Name: "name", Type: bigquery.StringFieldType, Required: true},
		{Name: "role",
			Type:     bigquery.RecordFieldType,
			Repeated: false,
			Schema: bigquery.Schema{
				{Name: "title", Type: bigquery.StringFieldType},
				{Name: "stage", Type: bigquery.StringFieldType},
				{Name: "etag", Type: bigquery.StringFieldType},
				{Name: "name", Type: bigquery.StringFieldType},
				{Name: "description", Type: bigquery.StringFieldType},
			}},
		{Name: "included_permissions", Type: bigquery.StringFieldType, Repeated: true},
	}

	permissionsSchema = bigquery.Schema{
		{Name: "name", Type: bigquery.StringFieldType, Required: true},
		{Name: "roles", Type: bigquery.StringFieldType, Repeated: true},
	}
)

func fronthandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("/ called")

	ctx := context.Background()
	crmService, err := cloudresourcemanager.NewService(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	iamService, err := iam.NewService(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	ors = iam.NewRolesService(iamService)

	limiter = rate.NewLimiter(rate.Limit(maxRequestsPerSecond), burst)

	switch *mode {
	case "organization":
		fmt.Printf("Getting Organization Roles/Permissions\n")

		if *organization == "" {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		oreq, err := crmService.Organizations.Get(fmt.Sprintf("organizations/%s", *organization)).Do()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Printf("     Organization Name %s\n", oreq.Name)
		*organization = oreq.Name

		parent := fmt.Sprintf(*organization)

		// A) for Organization Roles
		err = generateMap(ctx, parent)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

	case "project":
		// B) for Project Roles
		fmt.Printf("Getting Project Roles/Permissions\n")

		// TODO: only get projects in the selected organization
		preq := crmService.Projects.List()
		if err := preq.Pages(ctx, func(page *cloudresourcemanager.ListProjectsResponse) error {
			for _, p := range page.Projects {
				if p.LifecycleState == "ACTIVE" {
					projects = append(projects, p)
				}
			}
			return nil
		}); err != nil {
			fmt.Printf("Error Iterating projects, roles: %v\n", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		for _, p := range projects {
			parent := fmt.Sprintf("projects/%s", p.ProjectId)
			err = generateMap(ctx, parent)
			if err != nil {
				fmt.Printf("Error getting permissions, roles: %v\n", err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
	default:

		fmt.Printf("Getting Default Roles/Permissions\n")
		parent := ""
		err = generateMap(ctx, parent)
		if err != nil {
			fmt.Printf("Error getting default permissions, roles: %v\n", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	if len(roles.Roles) == 0 {
		roles.Roles = []Role{}
	}
	if len(permissions.Permissions) == 0 {
		permissions.Permissions = []Permission{}
	}

	fmt.Printf("%v\n", roles.Roles)
	fmt.Printf("%v\n", permissions.Permissions)
	fmt.Printf("Generating BigQuery output\n")

	// tokenSourceWithScopes, err := google.DefaultTokenSource(ctx, "https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/cloud-platform", "https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/devstorage.read_only")
	// if err != nil {
	// 	fmt.Printf("Error Creating BQ TokenSource %v\n", err)
	// 	http.Error(w, err.Error(), http.StatusInternalServerError)
	// 	return
	// }
	// bqClient, err := bigquery.NewClient(ctx, *bqProjectID, option.WithTokenSource(tokenSourceWithScopes))
	bqClient, err := bigquery.NewClient(ctx, *bqProjectID)
	if err != nil {
		fmt.Printf("Error Creating BQ Client %v\n", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	ds := bqClient.Dataset(*bqDataset)

	rolesTable := ds.Table(roleTableName)
	_, err = rolesTable.Metadata(ctx)
	if err != nil {
		fmt.Printf("Creating new roles table in dataset\n")
		err = rolesTable.Create(ctx, &bigquery.TableMetadata{
			TimePartitioning: &bigquery.TimePartitioning{
				Type: bigquery.DayPartitioningType,
			},
			Schema: rolesSchema,
		})
		if err != nil {
			fmt.Printf("Error Creating Roles Table %v\n", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	fmt.Printf("Uploading [%d] Roles\n", len(roles.Roles))

	var rlines []string
	for _, v := range roles.Roles {
		json, err := json.Marshal(v)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		rlines = append(rlines, string(json))
	}

	dataString := strings.Join(rlines, "\n")
	rolesSource := bigquery.NewReaderSource(strings.NewReader(dataString))

	rolesSource.SourceFormat = bigquery.JSON
	rolesSource.Schema = rolesSchema

	rloader := rolesTable.LoaderFrom(rolesSource)
	rloader.CreateDisposition = bigquery.CreateNever
	rjob, err := rloader.Run(ctx)
	if err != nil {
		fmt.Printf("Error loading Roles RUN to BQ with JobID %v\n", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	rstatus, err := rjob.Wait(ctx)
	if err != nil {
		fmt.Printf("Error loading Roles Wait to BQ jobID [%s]  %v\n", rjob.ID(), err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := rstatus.Err(); err != nil {
		fmt.Printf("Error loading Roles Status to BQ  jobID [%s]  %v\n", rjob.ID(), err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Printf("  Done  %t\n", rstatus.Done())

	permissionsTable := ds.Table(permissionsTableName)
	_, err = permissionsTable.Metadata(ctx)
	if err != nil {
		fmt.Printf("Creating new permissions table in dataset\n")
		err = permissionsTable.Create(ctx, &bigquery.TableMetadata{
			TimePartitioning: &bigquery.TimePartitioning{
				Type: bigquery.DayPartitioningType,
			},
			Schema: permissionsSchema,
		})
		if err != nil {
			fmt.Printf("Error Creating Permissions Table %v\n", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	fmt.Printf("Uploading [%d] Permissions\n", len(permissions.Permissions))

	var plines []string
	for _, v := range permissions.Permissions {
		json, err := json.Marshal(v)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		plines = append(plines, string(json))
	}

	dataString = strings.Join(plines, "\n")
	permissionsSource := bigquery.NewReaderSource(strings.NewReader(dataString))
	permissionsSource.SourceFormat = bigquery.JSON
	permissionsSource.Schema = permissionsSchema

	ploader := permissionsTable.LoaderFrom(permissionsSource)

	pjob, err := ploader.Run(ctx)
	if err != nil {
		fmt.Printf("Error loading Permissions RUN to BQ %v\n", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	pstatus, err := pjob.Wait(ctx)
	if err != nil {
		fmt.Printf("Error loading Permissions to BQ jobID [%s]  %v\n", pjob.ID(), err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := pstatus.Err(); err != nil {
		fmt.Printf("Error loading Permissions to BQ BQ jobID [%s]  %v\n", pjob.ID(), err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Printf("  Done  %t\n", pstatus.Done())

	fmt.Printf("done\n")

	fmt.Fprint(w, "ok")
}

func healthhandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("heathcheck...")
	fmt.Fprint(w, "ok")
}

func init() {
	flag.Parse()
}

func main() {
	http.HandleFunc("/", fronthandler)
	http.HandleFunc("/_ah/health", healthhandler)

	var server *http.Server
	server = &http.Server{
		Addr: ":8080",
	}
	http2.ConfigureServer(server, &http2.Server{})
	fmt.Println("Starting Server..")
	err := server.ListenAndServe()
	fmt.Printf("Unable to start Server %v", err)
}

func generateMap(ctx context.Context, parent string) error {
	var wg sync.WaitGroup

	oireq := ors.List().Parent(parent)
	if err := oireq.Pages(ctx, func(page *iam.ListRolesResponse) error {
		for _, sa := range page.Roles {
			wg.Add(1)
			go func(ctx context.Context, wg *sync.WaitGroup, sa *iam.Role) {
				defer wg.Done()
				var err error
				if err := limiter.Wait(ctx); err != nil {
					fmt.Printf("Error in rate limiter %v", err)
				}
				if ctx.Err() != nil {
					fmt.Printf("Error in rate limiter %v", err)
				}
				rc, err := ors.Get(sa.Name).Do()
				if err != nil {
					fmt.Printf("Error getting role name %v", err)
					return
				}
				cr := &Role{
					Name:                sa.Name,
					Role:                *sa,
					IncludedPermissions: rc.IncludedPermissions,
				}
				cmutex.Lock()
				_, ok := find(roles.Roles, sa.Name)
				if !ok {
					//fmt.Printf("     Iterating Role  %s\n", sa.Name)
					roles.Roles = append(roles.Roles, *cr)
				}
				cmutex.Unlock()
				//fmt.Printf("     Adding Permissions for Role  %s\n", sa.Name)
				for _, perm := range rc.IncludedPermissions {
					//fmt.Printf("     Appending Permission %s to Role %s", perm, sa.Name)
					i, ok := find(permissions.Permissions, perm)

					if !ok {
						pmutex.Lock()
						permissions.Permissions = append(permissions.Permissions, Permission{
							Name:  perm,
							Roles: []string{sa.Name},
						})
						pmutex.Unlock()
					} else {
						pmutex.Lock()
						p := permissions.Permissions[i]
						_, ok := find(p.Roles, sa.Name)
						if !ok {
							p.Roles = append(p.Roles, sa.Name)
							permissions.Permissions[i] = p
						}
						pmutex.Unlock()
					}

				}
			}(ctx, &wg, sa)

		}
		return nil
	}); err != nil {
		return err
	}

	wg.Wait()

	return nil
}

// generics....
func find(slice interface{}, val string) (int, bool) {

	switch slice.(type) {
	case []Role:
		for i, item := range slice.([]Role) {
			if item.Name == val {
				return i, true
			}
		}
	case []Permission:
		for i, item := range slice.([]Permission) {
			if item.Name == val {
				return i, true
			}
		}
	case []string:
		for i, item := range slice.([]string) {
			if item == val {
				return i, true
			}
		}
	default:
		return -1, false
	}

	return -1, false
}
