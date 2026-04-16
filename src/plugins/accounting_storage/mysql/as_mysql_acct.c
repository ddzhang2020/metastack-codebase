/*****************************************************************************\
 *  as_mysql_acct.c - functions dealing with accounts.
 *****************************************************************************
 *  Copyright (C) 2004-2007 The Regents of the University of California.
 *  Copyright (C) 2008-2010 Lawrence Livermore National Security.
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Written by Danny Auble <da@llnl.gov>
 *
 *  This file is part of Slurm, a resource management program.
 *  For details, see <https://slurm.schedmd.com/>.
 *  Please also read the included file: DISCLAIMER.
 *
 *  Slurm is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the License, or (at your option)
 *  any later version.
 *
 *  In addition, as a special exception, the copyright holders give permission
 *  to link the code of portions of this program with the OpenSSL library under
 *  certain conditions as described in each individual source file, and
 *  distribute linked combinations including the two. You must obey the GNU
 *  General Public License in all respects for all of the code used other than
 *  OpenSSL. If you modify file(s) with this exception, you may extend this
 *  exception to your version of the file(s), but you are not obligated to do
 *  so. If you do not wish to do so, delete this exception statement from your
 *  version.  If you delete this exception statement from all source files in
 *  the program, then also delete it here.
 *
 *  Slurm is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 *  FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 *  details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with Slurm; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301  USA.
\*****************************************************************************/

#include "as_mysql_assoc.h"
#include "as_mysql_acct.h"
#include "as_mysql_user.h"

#ifdef __METASTACK_OPT_SACCTMGR_ADD_USER
#include "as_mysql_usage.h"
#endif

#ifdef __METASTACK_ASSOC_HASH
#include "src/common/assoc_mgr.h"
#endif

typedef struct {
	slurmdb_account_rec_t *acct_in;
	slurmdb_assoc_rec_t *assoc_in;
	char *insert_query;
	char *insert_query_pos;
	mysql_conn_t *mysql_conn;
	time_t now;
	int rc;
	bool ret_str_err;
	char *ret_str;
	char *ret_str_pos;
	char *txn_query;
	char *txn_query_pos;
	char *user_name;
} add_acct_cond_t;

typedef struct {
	char *acct;
	list_t *acct_list;
	char *cluster_name;
	slurmdb_assoc_flags_t flags;
	mysql_conn_t *mysql_conn;
	char *query;
	char *query_pos;
	list_t *user_list;
} flag_coord_acct_t;

static int _foreach_flag_coord_handle(void *x, void *arg)
{
	slurmdb_user_rec_t *user_rec = x;
	flag_coord_acct_t *flag_coord_acct = arg;

	as_mysql_user_handle_user_coord_flag(
		user_rec,
		flag_coord_acct->flags,
		flag_coord_acct->acct);
	return 0;
}

static int _foreach_flag_coord_user(void *x, void *arg)
{
	slurmdb_assoc_rec_t *assoc_ptr = x;
	flag_coord_acct_t *flag_coord_acct = arg;
	int rc = 0;

	/* In the children_list the user assocs are always first */
	if (assoc_ptr->user) {
		slurmdb_user_rec_t *user_rec = as_mysql_user_add_coord_update(
			flag_coord_acct->mysql_conn,
			&flag_coord_acct->user_list,
			assoc_ptr->user,
			true);
		as_mysql_user_handle_user_coord_flag(
			user_rec,
			flag_coord_acct->flags,
			assoc_ptr->acct);
		return 0;
	}

	/*
	 * We have a non-user assoc, so add/remove that from the full user list.
	 */
	if (flag_coord_acct->user_list) {
		flag_coord_acct->acct = assoc_ptr->acct;
		rc = list_for_each(flag_coord_acct->user_list,
				   _foreach_flag_coord_handle,
				   flag_coord_acct);
		flag_coord_acct->acct = NULL;
	}

	if (assoc_ptr->usage->children_list)
		rc = list_for_each(assoc_ptr->usage->children_list,
				   _foreach_flag_coord_user,
				   flag_coord_acct);

	return rc;
}

static int _foreach_flag_coord_acct(void *x, void *arg)
{
	int rc = 1;
	flag_coord_acct_t *flag_coord_acct = arg;
	slurmdb_assoc_rec_t *assoc_ptr = NULL;
	slurmdb_assoc_rec_t assoc_req = {
		.cluster = flag_coord_acct->cluster_name,
		.acct = x,
		.uid = NO_VAL,
	};

	if (assoc_mgr_fill_in_assoc(flag_coord_acct->mysql_conn,
				     &assoc_req,
				     ACCOUNTING_ENFORCE_ASSOCS,
				     &assoc_ptr,
				     true) != SLURM_SUCCESS)
		return -1;
	/* Only change if needed */
	if (((assoc_ptr->flags & ASSOC_FLAG_USER_COORD) &&
	     (flag_coord_acct->flags & ASSOC_FLAG_USER_COORD_NO)) ||
	    (!(assoc_ptr->flags & ASSOC_FLAG_USER_COORD) &&
	     (flag_coord_acct->flags & ASSOC_FLAG_USER_COORD))) {
		slurmdb_assoc_rec_t *mod_assoc =
			xmalloc(sizeof(slurmdb_assoc_rec_t));
		slurmdb_init_assoc_rec(mod_assoc, 0);
		mod_assoc->id = assoc_ptr->id;
		mod_assoc->cluster = xstrdup(assoc_ptr->cluster);
		mod_assoc->flags = assoc_ptr->flags;
		if (flag_coord_acct->flags & ASSOC_FLAG_USER_COORD_NO)
			mod_assoc->flags &= ~ASSOC_FLAG_USER_COORD;
		else
			mod_assoc->flags |= ASSOC_FLAG_USER_COORD;

		if (addto_update_list(flag_coord_acct->mysql_conn->update_list,
				      SLURMDB_MODIFY_ASSOC, mod_assoc) !=
		    SLURM_SUCCESS) {
			error("Couldn't add removal of coord, this should never happen.");
			slurmdb_destroy_user_rec(mod_assoc);
			return 0;
		}

		/* set up query to remove the flag */
		if (!flag_coord_acct->query) {
			xstrfmtcatat(flag_coord_acct->query,
				     &flag_coord_acct->query_pos,
				     "update \"%s_%s\" set flags = %u where id_assoc IN (%u",
				     mod_assoc->cluster, assoc_table,
				     mod_assoc->flags, mod_assoc->id);
		} else {
			xstrfmtcatat(flag_coord_acct->query,
				     &flag_coord_acct->query_pos,
				     ",%u",
				     mod_assoc->id);
		}

		if (assoc_ptr->usage->children_list)
			rc = list_for_each(assoc_ptr->usage->children_list,
					   _foreach_flag_coord_user,
					   flag_coord_acct);
	}

	return rc;
}

static int _foreach_flag_coord_cluster(void *x, void *arg)
{
	int rc;
	flag_coord_acct_t *flag_coord_acct = arg;

	flag_coord_acct->cluster_name = x;

	rc = list_for_each_ro(flag_coord_acct->acct_list,
			      _foreach_flag_coord_acct,
			      flag_coord_acct);
	if (!rc)
		return rc;

	if (flag_coord_acct->query) {
		xstrcatat(flag_coord_acct->query,
			  &flag_coord_acct->query_pos,
			  ");");
		/* Now clear the flag for the associations in the database */
		DB_DEBUG(DB_ASSOC, flag_coord_acct->mysql_conn->conn,
			 "query\n%s",
			 flag_coord_acct->query);
		if ((rc = mysql_db_query(flag_coord_acct->mysql_conn,
					 flag_coord_acct->query)) !=
		    SLURM_SUCCESS) {
			error("Couldn't update flags");
			rc = 0;
		}

		xfree(flag_coord_acct->query);
	}

	return rc;
}

static void _handle_flag_coord(flag_coord_acct_t *flag_coord_acct)
{
	assoc_mgr_lock_t locks = {
		.assoc = READ_LOCK,
		.user = READ_LOCK,
#ifdef __METASTACK_ASSOC_HASH
		.uid = READ_LOCK,
#endif
	};

	assoc_mgr_lock(&locks);
	(void) list_for_each_ro(as_mysql_cluster_list,
				_foreach_flag_coord_cluster,
				flag_coord_acct);
	assoc_mgr_unlock(&locks);

	FREE_NULL_LIST(flag_coord_acct->user_list);
	xfree(flag_coord_acct->query);
}

static void _setup_acct_cond_limits(slurmdb_account_cond_t *acct_cond,
				    char **extra, char **at)
{
	list_itr_t *itr = NULL;
	char *object = NULL;

	if (!acct_cond)
		return;

	if (acct_cond->assoc_cond &&
	    acct_cond->assoc_cond->acct_list &&
	    list_count(acct_cond->assoc_cond->acct_list)) {
		int set = 0;
		xstrcatat(*extra, at, " && (");
		itr = list_iterator_create(acct_cond->assoc_cond->acct_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcatat(*extra, at, " || ");
			xstrfmtcatat(*extra, at, "name='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcat(*extra, ")");
	}

	if (acct_cond->description_list
	    && list_count(acct_cond->description_list)) {
		int set = 0;
		xstrcatat(*extra, at, " && (");
		itr = list_iterator_create(acct_cond->description_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcatat(*extra, at, " || ");
			xstrfmtcatat(*extra, at, "description='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcatat(*extra, at, ")");
	}

	if (acct_cond->flags != SLURMDB_ACCT_FLAG_NONE) {
		if (acct_cond->flags & SLURMDB_ACCT_FLAG_USER_COORD_NO) {
			xstrfmtcatat(*extra, at, " && !(flags & %u)",
				   SLURMDB_ACCT_FLAG_USER_COORD);
		} else if (acct_cond->flags & SLURMDB_ACCT_FLAG_USER_COORD) {
			xstrfmtcatat(*extra, at, " && (flags & %u)",
				   SLURMDB_ACCT_FLAG_USER_COORD);
		}
	}

	if (acct_cond->organization_list
	    && list_count(acct_cond->organization_list)) {
		int set = 0;
		xstrcatat(*extra, at, " && (");
		itr = list_iterator_create(acct_cond->organization_list);
		while ((object = list_next(itr))) {
			if (set)
				xstrcatat(*extra, at, " || ");
			xstrfmtcatat(*extra, at, "organization='%s'", object);
			set = 1;
		}
		list_iterator_destroy(itr);
		xstrcatat(*extra, at, ")");
	}

	return;
}

static int _foreach_add_acct(void *x, void *arg)
{
	char *name = x;
	add_acct_cond_t *add_acct_cond = arg;
	slurmdb_account_rec_t *acct;
	slurmdb_assoc_rec_t *assoc;
	char *desc;
	char *org;
	char *extra, *tmp_extra;
	MYSQL_RES *result = NULL;
	int cnt;
	char *query;
	slurmdb_acct_flags_t base_flags;

	/* Check to see if it is already in the acct_table */
	query = xstrdup_printf("select name from %s where name='%s' and !deleted",
			       acct_table, name);
	result = mysql_db_query_ret(add_acct_cond->mysql_conn, query, 0);

	xfree(query);
	if (!result)
		return -1;

	cnt = mysql_num_rows(result);
	mysql_free_result(result);
	/* If so, just return */
	if (cnt)
		return 0;

	/* Else, add it */
	acct = add_acct_cond->acct_in;
	assoc = add_acct_cond->assoc_in;
	desc = acct->description ? acct->description : x;
	org = acct->organization;

	if (!org) {
		if (assoc->parent_acct && xstrcmp(assoc->parent_acct, "root"))
			org = assoc->parent_acct;
		else
			org = x;
	}

	/* Clear flags we don't plan to store */
	base_flags = acct->flags & ~SLURMDB_ACCT_FLAG_BASE;

	if (!add_acct_cond->ret_str)
		xstrcatat(add_acct_cond->ret_str, &add_acct_cond->ret_str_pos,
			  " Adding Account(s)\n");

	xstrfmtcatat(add_acct_cond->ret_str, &add_acct_cond->ret_str_pos,
		     "  %s\n", name);

	if (add_acct_cond->insert_query)
		xstrfmtcatat(add_acct_cond->insert_query,
			     &add_acct_cond->insert_query_pos,
			     ", (%ld, %ld, '%s', '%s', '%s', %u)",
			     add_acct_cond->now, add_acct_cond->now,
			     name, desc, org, base_flags);
	else
		xstrfmtcatat(add_acct_cond->insert_query,
			     &add_acct_cond->insert_query_pos,
			     "insert into %s (creation_time, mod_time, name, description, organization, flags) values (%ld, %ld, '%s', '%s', '%s', %u)",
			     acct_table, add_acct_cond->now, add_acct_cond->now,
			     name, desc, org, base_flags);

	extra = xstrdup_printf("description='%s', organization='%s', flags='%u'",
			       desc, org, base_flags);
	tmp_extra = slurm_add_slash_to_quotes(extra);

	if (add_acct_cond->txn_query)
		xstrfmtcatat(add_acct_cond->txn_query,
			     &add_acct_cond->txn_query_pos,
			     ", (%ld, %u, '%s', '%s', '%s')",
			     add_acct_cond->now, DBD_ADD_ACCOUNTS, name,
			     add_acct_cond->user_name, tmp_extra);
	else
		xstrfmtcatat(add_acct_cond->txn_query,
			     &add_acct_cond->txn_query_pos,
			     "insert into %s (timestamp, action, name, actor, info) values (%ld, %u, '%s', '%s', '%s')",
			     txn_table,
			     add_acct_cond->now, DBD_ADD_ACCOUNTS, name,
			     add_acct_cond->user_name, tmp_extra);
	xfree(tmp_extra);
	xfree(extra);

	return 0;
}

extern int as_mysql_add_accts(mysql_conn_t *mysql_conn, uint32_t uid,
			      List acct_list)
{
	list_itr_t *itr = NULL;
	int rc = SLURM_SUCCESS;
	slurmdb_account_rec_t *object = NULL;
	char *cols = NULL, *vals = NULL, *query = NULL, *txn_query = NULL;
	time_t now = time(NULL);
	char *user_name = NULL;
	char *extra = NULL, *tmp_extra = NULL;

	int affect_rows = 0;
	List assoc_list;

	if (check_connection(mysql_conn) != SLURM_SUCCESS)
		return ESLURM_DB_CONNECTION;

	if (!is_user_min_admin_level(mysql_conn, uid, SLURMDB_ADMIN_OPERATOR)) {
		slurmdb_user_rec_t user;

		if (slurmdbd_conf->flags & DBD_CONF_FLAG_DISABLE_COORD_DBD) {
			error("Coordinator privilege revoked with DisableCoordDBD, only admins/operators can add accounts.");
			return ESLURM_ACCESS_DENIED;
		}

		memset(&user, 0, sizeof(slurmdb_user_rec_t));
		user.uid = uid;

		if (!is_user_any_coord(mysql_conn, &user)) {
			error("Only admins/operators/coordinators "
			      "can add accounts");
			return ESLURM_ACCESS_DENIED;
		}
		/* If the user is a coord of any acct they can add
		 * accounts they are only able to make associations to
		 * these accounts if they are coordinators of the
		 * parent they are trying to add to
		 */
	}

	if (!acct_list || !list_count(acct_list)) {
		error("%s: Trying to add empty account list", __func__);
		return ESLURM_EMPTY_LIST;
	}

	assoc_list = list_create(slurmdb_destroy_assoc_rec);
	user_name = uid_to_string((uid_t) uid);
	itr = list_iterator_create(acct_list);
	while ((object = list_next(itr))) {
		slurmdb_acct_flags_t base_flags;

		if (!object->name || !object->name[0]
		    || !object->description || !object->description[0]
		    || !object->organization || !object->organization[0]) {
			error("We need an account name, description, and "
			      "organization to add. %s %s %s",
			      object->name, object->description,
			      object->organization);
			rc = SLURM_ERROR;
			continue;
		}

		base_flags = object->flags & ~SLURMDB_ACCT_FLAG_BASE;

		xstrcat(cols, "creation_time, mod_time, name, "
			"description, organization, flags");
		xstrfmtcat(vals, "%ld, %ld, '%s', '%s', '%s', %u",
			   now, now, object->name,
			   object->description, object->organization,
			   base_flags);
		xstrfmtcat(extra, ", description='%s', organization='%s', flags=%u",
			   object->description, object->organization,
			   base_flags);

		query = xstrdup_printf(
			"insert into %s (%s) values (%s) "
			"on duplicate key update deleted=0, mod_time=%ld %s;",
			acct_table, cols, vals,
			now, extra);
		DB_DEBUG(DB_ASSOC, mysql_conn->conn, "query\n%s", query);
		rc = mysql_db_query(mysql_conn, query);
		xfree(cols);
		xfree(vals);
		xfree(query);
		if (rc != SLURM_SUCCESS) {
			error("Couldn't add acct");
			xfree(extra);
			continue;
		}
		affect_rows = last_affected_rows(mysql_conn);
		//DB_DEBUG(DB_ASSOC, mysql_conn->conn, "affected %d",
		//         affect_rows);

		if (!affect_rows) {
			DB_DEBUG(DB_ASSOC, mysql_conn->conn, "nothing changed");
			xfree(extra);
			continue;
		}

		/* we always have a ', ' as the first 2 chars */
		tmp_extra = slurm_add_slash_to_quotes(extra+2);

		if (txn_query)
			xstrfmtcat(txn_query,
				   ", (%ld, %u, '%s', '%s', '%s')",
				   now, DBD_ADD_ACCOUNTS, object->name,
				   user_name, tmp_extra);
		else
			xstrfmtcat(txn_query,
				   "insert into %s "
				   "(timestamp, action, name, actor, info) "
				   "values (%ld, %u, '%s', '%s', '%s')",
				   txn_table,
				   now, DBD_ADD_ACCOUNTS, object->name,
				   user_name, tmp_extra);
		xfree(tmp_extra);
		xfree(extra);

		if (!object->assoc_list)
			continue;

		if (!assoc_list)
			assoc_list =
				list_create(slurmdb_destroy_assoc_rec);
		list_transfer(assoc_list, object->assoc_list);
	}
	list_iterator_destroy(itr);
	xfree(user_name);

	if (rc != SLURM_ERROR) {
		if (txn_query) {
			xstrcat(txn_query, ";");
			rc = mysql_db_query(mysql_conn,
					    txn_query);
			xfree(txn_query);
			if (rc != SLURM_SUCCESS) {
				error("Couldn't add txn");
				rc = SLURM_SUCCESS;
			}
		}
	} else
		xfree(txn_query);

	if (assoc_list && list_count(assoc_list)) {
		if ((rc = as_mysql_add_assocs(mysql_conn, uid, assoc_list))
		    != SLURM_SUCCESS)
			error("Problem adding accounts associations");
	}
	FREE_NULL_LIST(assoc_list);

	return rc;
}

extern char *as_mysql_add_accts_cond(mysql_conn_t *mysql_conn, uint32_t uid,
				     slurmdb_add_assoc_cond_t *add_assoc,
				     slurmdb_account_rec_t *acct)
{
	add_acct_cond_t add_acct_cond;
	char *ret_str = NULL;
	int rc;

	if (check_connection(mysql_conn) != SLURM_SUCCESS) {
		errno = ESLURM_DB_CONNECTION;
		return NULL;
	}

	if (!add_assoc ||
	    !add_assoc->acct_list ||
	    !list_count(add_assoc->acct_list)) {
		errno = ESLURM_EMPTY_LIST;
		return NULL;
	}

	if (!is_user_min_admin_level(mysql_conn, uid, SLURMDB_ADMIN_OPERATOR)) {
		slurmdb_user_rec_t user;

		if (slurmdbd_conf->flags & DBD_CONF_FLAG_DISABLE_COORD_DBD) {
			char *ret_str = xstrdup("Coordinator privilege revoked with DisableCoordDBD, only admins/operators can add accounts.");
			error("%s", ret_str);
			errno = ESLURM_ACCESS_DENIED;
			return ret_str;
		}

		memset(&user, 0, sizeof(slurmdb_user_rec_t));
		user.uid = uid;

		if (!is_user_any_coord(mysql_conn, &user)) {
			char *ret_str = xstrdup("Only admins/operators/coordinators can add accounts");
			error("%s", ret_str);
			errno = ESLURM_ACCESS_DENIED;
			return ret_str;
		}
		/*
		 * If the user is a coord of any acct they can add
		 * accounts they are only able to make associations to
		 * these accounts if they are coordinators of the
		 * parent they are trying to add to
		 */
	}

	/* Transfer over relavant flags from the account to the association. */
	if (acct->flags & SLURMDB_ACCT_FLAG_USER_COORD)
		add_assoc->assoc.flags |= ASSOC_FLAG_USER_COORD;

	memset(&add_acct_cond, 0, sizeof(add_acct_cond));
	add_acct_cond.acct_in = acct;
	add_acct_cond.assoc_in = &add_assoc->assoc;
	add_acct_cond.mysql_conn = mysql_conn;
	add_acct_cond.now = time(NULL);
	add_acct_cond.user_name = uid_to_string((uid_t) uid);

	/* First add the accounts to the acct_table. */
	if (list_for_each_ro(add_assoc->acct_list, _foreach_add_acct,
			     &add_acct_cond) < 0) {
		rc = add_acct_cond.rc;
		goto end_it;
	}

	if (add_acct_cond.insert_query) {
		xstrfmtcatat(add_acct_cond.insert_query,
			     &add_acct_cond.insert_query_pos,
			     " on duplicate key update deleted=0, description=VALUES(description), mod_time=VALUES(mod_time), organization=VALUES(organization);");
		DB_DEBUG(DB_ASSOC, mysql_conn->conn, "query\n%s",
			 add_acct_cond.insert_query);
		rc = mysql_db_query(mysql_conn, add_acct_cond.insert_query);
		xfree(add_acct_cond.insert_query);
		if (rc != SLURM_SUCCESS) {
			error("Couldn't add acct");
			xfree(add_acct_cond.ret_str);
			goto end_it;
		}

		/* Success means we add the defaults to the string */
		xstrfmtcatat(add_acct_cond.ret_str,
			     &add_acct_cond.ret_str_pos,
			     " Settings\n  Description     = %s\n  Organization    = %s\n",
			     acct->description ?
			     acct->description : "Account Name",
			     acct->organization ?
			     acct->organization : "Parent/Account Name");

		xstrcatat(add_acct_cond.txn_query,
			  &add_acct_cond.txn_query_pos,
			  ";");
		rc = mysql_db_query(mysql_conn, add_acct_cond.txn_query);
		if (rc != SLURM_SUCCESS) {
			error("Couldn't add txn");
			rc = SLURM_SUCCESS;
		}
	}

	/* Now add the associations */
	ret_str = as_mysql_add_assocs_cond(mysql_conn, uid, add_assoc);
	rc = errno;

	if (rc == SLURM_NO_CHANGE_IN_DATA) {
		if (add_acct_cond.ret_str)
			rc = SLURM_SUCCESS;
	}

end_it:
	xfree(add_acct_cond.insert_query);
	xfree(add_acct_cond.txn_query);
	xfree(add_acct_cond.user_name);

	if (rc != SLURM_SUCCESS) {
		reset_mysql_conn(mysql_conn);
		if (!add_acct_cond.ret_str_err)
			xfree(add_acct_cond.ret_str);
		else
			xfree(ret_str);
		errno = rc;
		return add_acct_cond.ret_str ? add_acct_cond.ret_str : ret_str;
	}

	if (ret_str) {
		xstrcatat(add_acct_cond.ret_str,
			  &add_acct_cond.ret_str_pos,
			  ret_str);
		xfree(ret_str);
	}

	if (!add_acct_cond.ret_str) {
		DB_DEBUG(DB_ASSOC, mysql_conn->conn, "didn't affect anything");
		errno = SLURM_NO_CHANGE_IN_DATA;
		return NULL;
	}

	errno = SLURM_SUCCESS;
	return add_acct_cond.ret_str;
}

extern List as_mysql_modify_accts(mysql_conn_t *mysql_conn, uint32_t uid,
				  slurmdb_account_cond_t *acct_cond,
				  slurmdb_account_rec_t *acct)
{
	List ret_list = NULL;
	int rc = SLURM_SUCCESS;
	char *object = NULL, *at = NULL;
	char *vals = NULL, *extra = NULL, *query = NULL, *name_char = NULL;
	time_t now = time(NULL);
	char *user_name = NULL;
	slurmdb_assoc_flags_t assoc_flags = ASSOC_FLAG_NONE;
	MYSQL_RES *result = NULL;
	MYSQL_ROW row;

	if (!acct_cond || !acct) {
		error("we need something to change");
		return NULL;
	}

	if (check_connection(mysql_conn) != SLURM_SUCCESS)
		return NULL;

	if (!is_user_min_admin_level(mysql_conn, uid, SLURMDB_ADMIN_OPERATOR)) {
		errno = ESLURM_ACCESS_DENIED;
		return NULL;
	}

	xstrcatat(extra, &at, "where deleted=0");

	_setup_acct_cond_limits(acct_cond, &extra, &at);

	if (acct->description)
		xstrfmtcat(vals, ", description='%s'", acct->description);
	if (acct->organization)
		xstrfmtcat(vals, ", organization='%s'", acct->organization);

	if (acct->flags & SLURMDB_ACCT_FLAG_USER_COORD_NO) {
		xstrfmtcat(vals, ", flags=flags&~%u",
			   SLURMDB_ACCT_FLAG_USER_COORD);
		assoc_flags |= ASSOC_FLAG_USER_COORD_NO;
	} else if (acct->flags & SLURMDB_ACCT_FLAG_USER_COORD) {
		xstrfmtcat(vals, ", flags=flags|%u",
			   SLURMDB_ACCT_FLAG_USER_COORD);
		assoc_flags |= ASSOC_FLAG_USER_COORD;
	}

	if (!extra || !vals) {
		errno = SLURM_NO_CHANGE_IN_DATA;
		error("Nothing to change");
		return NULL;
	}

	query = xstrdup_printf("select name from %s %s;", acct_table, extra);
	xfree(extra);
	DB_DEBUG(DB_ASSOC, mysql_conn->conn, "query\n%s", query);
	if (!(result = mysql_db_query_ret(
		      mysql_conn, query, 0))) {
		xfree(query);
		xfree(vals);
		return NULL;
	}

	rc = 0;
	ret_list = list_create(xfree_ptr);
	while ((row = mysql_fetch_row(result))) {
		object = xstrdup(row[0]);
		list_append(ret_list, object);
		if (!rc) {
			xstrfmtcat(name_char, "(name='%s'", object);
			rc = 1;
		} else  {
			xstrfmtcat(name_char, " || name='%s'", object);
		}

	}
	mysql_free_result(result);

	if (!list_count(ret_list)) {
		errno = SLURM_NO_CHANGE_IN_DATA;
		DB_DEBUG(DB_ASSOC, mysql_conn->conn,
		         "didn't affect anything\n%s", query);
		xfree(query);
		xfree(vals);
		return ret_list;
	}
	xfree(query);
	xstrcat(name_char, ")");

	user_name = uid_to_string((uid_t) uid);
	rc = modify_common(mysql_conn, DBD_MODIFY_ACCOUNTS, now,
			   user_name, acct_table, name_char, vals, NULL);
	xfree(user_name);
	if (rc == SLURM_ERROR) {
		error("Couldn't modify accounts");
		FREE_NULL_LIST(ret_list);
		errno = SLURM_ERROR;
		ret_list = NULL;
	}

	xfree(name_char);
	xfree(vals);

	if (ret_list &&
	    (assoc_flags &
	     (ASSOC_FLAG_USER_COORD_NO | ASSOC_FLAG_USER_COORD))) {
		flag_coord_acct_t flag_coord_acct = {
			.acct_list = ret_list,
			.flags = assoc_flags,
			.mysql_conn = mysql_conn,
		};

		/* Update associations based on account flags */
		_handle_flag_coord(&flag_coord_acct);
	}

	return ret_list;
}

extern List as_mysql_remove_accts(mysql_conn_t *mysql_conn, uint32_t uid,
				  slurmdb_account_cond_t *acct_cond)
{
	list_itr_t *itr = NULL;
	List ret_list = NULL;
	List coord_list = NULL;
	List cluster_list_tmp = NULL;
	int rc = SLURM_SUCCESS;
	char *object = NULL, *at = NULL;
	char *extra = NULL, *query = NULL,
		*name_char = NULL, *name_char_pos = NULL,
		*assoc_char = NULL, *assoc_char_pos = NULL;
	time_t now = time(NULL);
	char *user_name = NULL;
	MYSQL_RES *result = NULL;
	MYSQL_ROW row;
	bool jobs_running = 0;
	bool default_account = 0;

	if (!acct_cond) {
		error("we need something to change");
		return NULL;
	}

	if (check_connection(mysql_conn) != SLURM_SUCCESS)
		return NULL;

	if (!is_user_min_admin_level(mysql_conn, uid, SLURMDB_ADMIN_OPERATOR)) {
		errno = ESLURM_ACCESS_DENIED;
		return NULL;
	}

	xstrcatat(extra, &at, "where deleted=0");

	_setup_acct_cond_limits(acct_cond, &extra, &at);

	if (!extra) {
		error("Nothing to remove");
		return NULL;
	}

	query = xstrdup_printf("select name from %s %s;", acct_table, extra);
	xfree(extra);
	if (!(result = mysql_db_query_ret(
		      mysql_conn, query, 0))) {
		xfree(query);
		return NULL;
	}

	ret_list = list_create(xfree_ptr);
	while ((row = mysql_fetch_row(result))) {
		char *object = xstrdup(row[0]);
		list_append(ret_list, object);

		if (name_char)
			xstrfmtcatat(name_char, &name_char_pos, ",'%s'",
				     object);
		else
			xstrfmtcatat(name_char, &name_char_pos, "name in('%s'",
				     object);
		xstrfmtcatat(assoc_char, &assoc_char_pos,
			     "%st2.lineage like '%%/%s/%%'",
			     assoc_char ? " || " : "", object);
	}

	if (name_char)
		xstrcatat(name_char, &name_char_pos, ")");

	mysql_free_result(result);

	if (!list_count(ret_list)) {
		errno = SLURM_NO_CHANGE_IN_DATA;
		DB_DEBUG(DB_ASSOC, mysql_conn->conn,
		         "didn't affect anything\n%s", query);
		xfree(query);
		return ret_list;
	}
	xfree(query);

	/* We need to remove these accounts from the coord's that have it */
	coord_list = as_mysql_remove_coord(
		mysql_conn, uid, ret_list, NULL);
	FREE_NULL_LIST(coord_list);

	user_name = uid_to_string((uid_t) uid);

	slurm_rwlock_rdlock(&as_mysql_cluster_list_lock);
	cluster_list_tmp = list_shallow_copy(as_mysql_cluster_list);
	itr = list_iterator_create(cluster_list_tmp);
	while ((object = list_next(itr))) {
#ifdef __METASTACK_OPT_SACCTMGR_ADD_USER
		uint32_t rpc_version = get_cluster_version(mysql_conn, object);
		if (rpc_version < SLURM_23_11_PROTOCOL_VERSION) {
			slurm_mutex_lock(&assoc_lock);
		}

		rc = remove_common(mysql_conn, DBD_REMOVE_ACCOUNTS, now,
					user_name, acct_table, name_char,
					assoc_char, object, ret_list,
					&jobs_running, &default_account);

		if (rpc_version < SLURM_23_11_PROTOCOL_VERSION) {
			slurm_mutex_unlock(&assoc_lock);
		}

		if (rc != SLURM_SUCCESS)
			break;
#endif
	}
	list_iterator_destroy(itr);
	FREE_NULL_LIST(cluster_list_tmp);
	slurm_rwlock_unlock(&as_mysql_cluster_list_lock);

	xfree(user_name);
	xfree(name_char);
	xfree(assoc_char);
	if (rc == SLURM_ERROR) {
		FREE_NULL_LIST(ret_list);
		return NULL;
	}

	if (default_account)
		errno = ESLURM_NO_REMOVE_DEFAULT_ACCOUNT;
	else if (jobs_running)
		errno = ESLURM_JOBS_RUNNING_ON_ASSOC;
	else
		errno = SLURM_SUCCESS;
	return ret_list;
}

#ifdef __METASTACK_OPT_USER_DEACTIVATE
static int _foreach_activate_acct(void *x, void *arg)
{
	char *name = x;
	add_acct_cond_t *activate_acct_cond = arg;
	slurmdb_account_rec_t *acct = NULL;
	char *extra, *vals = NULL;
	MYSQL_RES *result = NULL;
	MYSQL_ROW row = NULL;
	char *query	= NULL;
	slurmdb_assoc_flags_t assoc_flags = ASSOC_FLAG_NONE;
	int deleted_val = -1;

	/* Check the account */
	query = xstrdup_printf("select deleted from %s where name='%s'", acct_table, name);
	result = mysql_db_query_ret(activate_acct_cond->mysql_conn, query, 0);

	xfree(query);
	if (!result)
		return -1;

	row = mysql_fetch_row(result);
	if (!row) {
		/* Account does not exist */
		if (!activate_acct_cond->ret_str)
			xstrcatat(activate_acct_cond->ret_str, &activate_acct_cond->ret_str_pos,
			      " Activating Account(s)\n");
		xstrfmtcatat(activate_acct_cond->ret_str, &activate_acct_cond->ret_str_pos,
			     "  Account %s does not exist\n", name);
		mysql_free_result(result);
		return 0;
	}

	deleted_val = slurm_atoul(row[0]);
	mysql_free_result(result);

	if (deleted_val == 0) {
		if (!activate_acct_cond->ret_str)
			xstrcatat(activate_acct_cond->ret_str, &activate_acct_cond->ret_str_pos,
			      " Activating Account(s)\n");
		xstrfmtcatat(activate_acct_cond->ret_str, &activate_acct_cond->ret_str_pos,
			     "  Account %s exists and is active\n", name);
		return 0;
	} else if (deleted_val == 1) {
		if (!activate_acct_cond->ret_str)
			xstrcatat(activate_acct_cond->ret_str, &activate_acct_cond->ret_str_pos,
			      " Activating Account(s)\n");
		xstrfmtcatat(activate_acct_cond->ret_str, &activate_acct_cond->ret_str_pos,
			     "  Account %s has been deleted\n", name);
		return 0;
	} 

	/* Otherwise assume deleted==5 and continue activation */
	acct = activate_acct_cond->acct_in;

	vals = xstrdup("deleted=0");
	if (acct->description)
		xstrfmtcat(vals, ", description='%s'", acct->description);
	if (acct->organization)
		xstrfmtcat(vals, ", organization='%s'", acct->organization);

	if (acct->flags & SLURMDB_ACCT_FLAG_USER_COORD_NO) {
		xstrfmtcat(vals, ", flags=flags&~%u",
			   SLURMDB_ACCT_FLAG_USER_COORD);
	} else if (acct->flags & SLURMDB_ACCT_FLAG_USER_COORD) {
		xstrfmtcat(vals, ", flags=flags|%u",
			   SLURMDB_ACCT_FLAG_USER_COORD);
	}

	if (!activate_acct_cond->ret_str)
		xstrcatat(activate_acct_cond->ret_str, &activate_acct_cond->ret_str_pos,
			  " Activating Account(s)\n");

	xstrfmtcatat(activate_acct_cond->ret_str, &activate_acct_cond->ret_str_pos,
		     "  Activate account %s\n", name);

	if (activate_acct_cond->insert_query)
		xstrfmtcatat(activate_acct_cond->insert_query,
			     &activate_acct_cond->insert_query_pos,
			     "; update %s set mod_time=%ld, %s where name='%s' and deleted=%d",
			     acct_table, activate_acct_cond->now, vals, name, SLURMDB_USER_DEACTIVATED);
	else
		xstrfmtcatat(activate_acct_cond->insert_query,
			     &activate_acct_cond->insert_query_pos,
			     "update %s set mod_time=%ld, %s where name='%s' and deleted=%d",
			     acct_table, activate_acct_cond->now, vals, name, SLURMDB_USER_DEACTIVATED);

	extra = slurm_add_slash_to_quotes(vals);

	if (activate_acct_cond->txn_query)
		xstrfmtcatat(activate_acct_cond->txn_query,
			     &activate_acct_cond->txn_query_pos,
			     ", (%ld, %u, '%s', '%s', '%s')",
			     activate_acct_cond->now, DBD_ACTIVATE_ACCOUNTS_COND, name,
			     activate_acct_cond->user_name, extra);
	else
		xstrfmtcatat(activate_acct_cond->txn_query,
			     &activate_acct_cond->txn_query_pos,
			     "insert into %s (timestamp, action, name, actor, info) values (%ld, %u, '%s', '%s', '%s')",
			     txn_table,
			     activate_acct_cond->now, DBD_ACTIVATE_ACCOUNTS_COND, name,
			     activate_acct_cond->user_name, extra);
	xfree(vals);
	xfree(extra);

	return 0;
}

static int _activate_assoc_cond_cluster(void *x, void *arg)
{
	add_assoc_cond_t *activate_assoc_cond = arg;
	int set = 0, cnt = 0;
	char *object = NULL, *extra = NULL, *prefix = "t1", *query = NULL, *fields = NULL;
	list_itr_t *itr = NULL;
	MYSQL_RES *result = NULL;
	MYSQL_ROW row = NULL;
	mysql_conn_t *mysql_conn = activate_assoc_cond->mysql_conn;

	activate_assoc_cond->add_assoc->assoc.cluster = x;
	activate_assoc_cond->rpc_version = get_cluster_version(
		activate_assoc_cond->mysql_conn,
		activate_assoc_cond->add_assoc->assoc.cluster);

	slurmdb_add_assoc_cond_t *activate_assoc = activate_assoc_cond->add_assoc;
    if (activate_assoc->acct_list && list_count(activate_assoc->acct_list)) {
        set = 0;
        xstrcat(extra, " && (");
        itr = list_iterator_create(activate_assoc->acct_list);
        while ((object = list_next(itr))) {
            if (set)
                xstrcat(extra, " || ");
            xstrfmtcat(extra, "%s.acct='%s'",
                   prefix, object);
            }
            set = 1;
        }
        list_iterator_destroy(itr);
        xstrcat(extra, ")");
    } else {
		error("we need something to activate");
	}

	if (!activate_assoc->user_list) {
		debug4("no user specified looking at accounts");
		xstrcat(extra, " && user = '' ");
	}

	/* Check the account association */
	query = xstrdup_printf("select acct,deleted from %s_%s where %s", 
				x, assoc_table, extra);
	result = mysql_db_query_ret(mysql_conn, query, 0);

	xfree(query);
	if (!result)
		return -1;

	cnt = mysql_num_rows(result);
	if (!cnt) {
		error("Account does not exist in cluster %s", x);
		mysql_free_result(result);
		return 0;
	}
	while ((row = mysql_fetch_row(result))) {
		if (slurm_atoul(row[1]) == 0) {
			if (!activate_assoc_cond->ret_str)
				xstrcatat(activate_assoc_cond->ret_str, &activate_assoc_cond->ret_str_pos,
			      " Activating account association(s)\n");
			xstrfmtcatat(activate_assoc_cond->ret_str, &activate_assoc_cond->ret_str_pos,
			      "  Account %s in cluster %s is active, cannot activate association",
			      row[0], x);
		} else if (slurm_atoul(row[1]) == 1) {
			if (!activate_assoc_cond->ret_str)
				xstrcatat(activate_assoc_cond->ret_str, &activate_assoc_cond->ret_str_pos,
			      " Activating account association(s)\n");
			xstrfmtcatat(activate_assoc_cond->ret_str, &activate_assoc_cond->ret_str_pos,
			      "  Account %s in cluster %s has been deleted, cannot activate association",
			      row[0], x);
		} else if (slurm_atoul(row[1]) == 5) {
			if (!activate_assoc_cond->ret_str)
				xstrcatat(activate_assoc_cond->ret_str, &activate_assoc_cond->ret_str_pos,
			      " Activating account association(s)\n");
			xstrfmtcatat(activate_assoc_cond->ret_str, &activate_assoc_cond->ret_str_pos,
			      "  Account %s in cluster %s is deactivated, can activate association",
			      row[0], x);
		}
	}
	mysql_free_result(result);


	/* activate the association */
	ime_t now = time(NULL);  
	query = xstrdup_printf("update %s_%s set mod_time=%ld, deleted=0 where %s && deleted=%d",
			     x, assoc_table, now, extra, SLURMDB_USER_DEACTIVATED);
	DB_DEBUG(DB_ASSOC, mysql_conn->conn, "query\n%s", query);
	if (mysql_db_query(mysql_conn, query) != SLURM_SUCCESS) {
		error("Couldn't activate account association for cluster %s", x);
		xfree(query);
		xfree(extra);
		return -1;
	}
	xfree(query);

	query = xstrdup_printf("insert into %s (timestamp, action, name, actor, info) values (%ld, %u, '%s', '%s', '%s')",
			     txn_table,
			     now, DBD_ACTIVATE_ASSOC, name,
			     activate_assoc_cond->user_name, extra);
	DB_DEBUG(DB_ASSOC, mysql_conn->conn, "query\n%s", query);
	if (mysql_db_query(mysql_conn, query) != SLURM_SUCCESS) {
		error("Couldn't add txn");
		xfree(query);
		xfree(extra);
		return -1;
	}
	xfree(query);

	


	if (list_for_each_ro(activate_assoc_cond->add_assoc->acct_list,
			     _add_assoc_cond_acct,
			     activate	_assoc_cond) < 0)
		goto end_it;

	_post_add_assoc_cond_cluster(activate_assoc_cond);

end_it:
#ifdef __METASTACK_OPT_SACCTMGR_ADD_USER
	if (locked) {
		slurm_mutex_unlock(&assoc_lock);
	}
#endif
	xfree(activate_assoc_cond->old_parent);
	xfree(activate_assoc_cond->old_cluster);
	activate_assoc_cond->add_assoc->assoc.cluster = NULL;
	if (activate_assoc_cond->rc != SLURM_SUCCESS)
		return -1;
	else
		return 0;
}

extern char *as_mysql_activate_assocs_cond(mysql_conn_t *mysql_conn, uint32_t uid,
				      slurmdb_add_assoc_cond_t *activate_assoc)
{
	int rc = SLURM_SUCCESS;
	list_t *use_cluster_list = NULL;
	add_assoc_cond_t activate_assoc_cond = NULL;
	assoc_mgr_lock_t locks = {
		.assoc = READ_LOCK,
		.user = READ_LOCK,
#ifdef __METASTACK_ASSOC_HASH
		.uid = READ_LOCK,
#endif
		.qos = READ_LOCK,
	};

	if (!activate_assoc) {
		error("we need something to activate");
		return NULL;
	}

	if (check_connection(mysql_conn) != SLURM_SUCCESS)
		return NULL;

	memset(&activate_assoc_cond, 0, sizeof(activate_assoc_cond));

	assoc_mgr_lock(&locks);
	activate_assoc_cond.assoc_mgr_locked = true;
	activate_assoc_cond.flags = ASSOC_FLAG_USER_COORD;

	if (!is_user_min_admin_level_locked(mysql_conn, uid,
					    SLURMDB_ADMIN_OPERATOR)) {
		slurmdb_user_rec_t user;

		if (slurmdbd_conf->flags & DBD_CONF_FLAG_DISABLE_COORD_DBD) {
			error("Coordinator privilege revoked with DisableCoordDBD, only admins/operators can add associations.");
			assoc_mgr_unlock(&locks);
			errno = ESLURM_ACCESS_DENIED;
			return NULL;
		}

		memset(&user, 0, sizeof(slurmdb_user_rec_t));
		user.uid = uid;

		if (!is_user_any_coord_locked(mysql_conn, &user)) {
			error("Only admins/operators/coordinators can add associations");
			assoc_mgr_unlock(&locks);
			errno = ESLURM_ACCESS_DENIED;
			return NULL;
		}

		if (activate_assoc->user_list)
			rc = list_for_each_ro(activate_assoc->acct_list,
					      _foreach_is_coord,
					      &user);
		else
			rc = _foreach_is_coord(activate_assoc->assoc.parent_acct,
					       &user);

		if (rc < 0) {
			error("Coordinator %s(%d) tried to activate associations where they were not allowed",
			      user.name, user.uid);
			assoc_mgr_unlock(&locks);
			errno = ESLURM_ACCESS_DENIED;
			return NULL;
		}

		activate_assoc_cond.is_coord = true;
	}

	if ((rc = setup_assoc_limits_locked(&activate_assoc->assoc,
					    &activate_assoc_cond.cols,
					    &activate_assoc_cond.vals,
					    &activate_assoc_cond.extra,
					    QOS_LEVEL_MODIFY,
					    0))) {
		xfree(activate_assoc_cond.cols);
		xfree(activate_assoc_cond.extra);
		xfree(activate_assoc_cond.vals);
		xfree(extra);
		errno = rc;
		error("%s: Failed, setup_assoc_limits functions returned error",
		      __func__);
		assoc_mgr_unlock(&locks);
		return NULL;
	}

	if (activate_assoc->cluster_list && list_count(activate_assoc->cluster_list))
		use_cluster_list = activate_assoc->cluster_list;
	else
		/*
		 * No need to do a shallow copy here as we are doing a
		 * list_for_each_ro() which will handle the locks for us.
		 */
		use_cluster_list = as_mysql_cluster_list;

	activate_assoc_cond.add_assoc = activate_assoc;
	activate_assoc_cond.mysql_conn = mysql_conn;
	activate_assoc_cond.uid = uid;
	activate_assoc_cond.user_name = uid_to_string((uid_t) uid);

	(void) list_for_each_ro(use_cluster_list, _activate_assoc_cond_cluster,
				&activate_assoc_cond);
	assoc_mgr_unlock(&locks);

	xfree(activate_assoc_cond.cols);
	xfree(activate_assoc_cond.extra);
	FREE_NULL_LIST(activate_assoc_cond.coord_users);

	if (activate_assoc_cond.rc != SLURM_SUCCESS) {
		reset_mysql_conn(mysql_conn);
		if (!(activate_assoc_cond.flags & ADD_ASSOC_FLAG_STR_ERR))
			xfree(activate_assoc_cond.ret_str);
		errno = activate_assoc_cond.rc;
	} else if (!(activate_assoc_cond.flags & ADD_ASSOC_FLAG_ADDED)) {
		DB_DEBUG(DB_ASSOC, mysql_conn->conn, "didn't affect anything");
		errno = SLURM_NO_CHANGE_IN_DATA;
	} else {
		if ((activate_assoc_cond.rc == SLURM_SUCCESS) &&
		    activate_assoc_cond.txn_query) {
			xstrcat(activate_assoc_cond.txn_query, ";");
			debug4("%d(%s:%d) query\n%s",
			       mysql_conn->conn, THIS_FILE,
			       __LINE__, activate_assoc_cond.txn_query);
			rc = mysql_db_query(mysql_conn,
					    activate_assoc_cond.txn_query);
			if (rc != SLURM_SUCCESS) {
				error("Couldn't add txn");
				rc = SLURM_SUCCESS;
			}
		}
		errno = SLURM_SUCCESS;
	}
	xfree(activate_assoc_cond.txn_query);
	xfree(activate_assoc_cond.user_name);
	xfree(activate_assoc_cond.vals);

	return activate_assoc_cond.ret_str;
}

extern char *as_mysql_activate_accts_cond(mysql_conn_t *mysql_conn, uint32_t uid,
				     slurmdb_add_assoc_cond_t *activate_assoc,
				     slurmdb_account_rec_t *acct)
{
	add_acct_cond_t activate_acct_cond;
	char *ret_str = NULL;
	int rc;

	if (check_connection(mysql_conn) != SLURM_SUCCESS) {
		errno = ESLURM_DB_CONNECTION;
		return NULL;
	}

	if (!activate_assoc ||
	    !activate_assoc->acct_list ||
	    !list_count(activate_assoc->acct_list)) {
		errno = ESLURM_EMPTY_LIST;
		return NULL;
	}

	if (!is_user_min_admin_level(mysql_conn, uid, SLURMDB_ADMIN_OPERATOR)) {
		slurmdb_user_rec_t user;

		if (slurmdbd_conf->flags & DBD_CONF_FLAG_DISABLE_COORD_DBD) {
			char *ret_str = xstrdup("Coordinator privilege revoked with DisableCoordDBD, only admins/operators can activate accounts.");
			error("%s", ret_str);
			errno = ESLURM_ACCESS_DENIED;
			return ret_str;
		}

		memset(&user, 0, sizeof(slurmdb_user_rec_t));
		user.uid = uid;

		if (!is_user_any_coord(mysql_conn, &user)) {
			char *ret_str = xstrdup("Only admins/operators/coordinators can activate accounts");
			error("%s", ret_str);
			errno = ESLURM_ACCESS_DENIED;
			return ret_str;
		}
		/*
		 * If the user is a coord of any acct they can activate
		 * accounts they are only able to make associations to
		 * these accounts if they are coordinators of the
		 * parent they are trying to activate to
		 */
	}

	/* Transfer over relavant flags from the account to the association. */
	if (acct->flags & SLURMDB_ACCT_FLAG_USER_COORD)
		activate_assoc->assoc.flags |= ASSOC_FLAG_USER_COORD;

	memset(&activate_acct_cond, 0, sizeof(activate_acct_cond));
	activate_acct_cond.acct_in = acct;
	activate_acct_cond.assoc_in = &activate_assoc->assoc;
	activate_acct_cond.mysql_conn = mysql_conn;
	activate_acct_cond.now = time(NULL);
	activate_acct_cond.user_name = uid_to_string((uid_t) uid);

	/* First activate the accounts to the acct_table. */
	if (list_for_each_ro(activate_assoc->acct_list, _foreach_activate_acct,
			     &activate_acct_cond) < 0) {
		rc = activate_acct_cond.rc;
		goto end_it;
	}

	if (activate_acct_cond.insert_query) {
		DB_DEBUG(DB_ASSOC, mysql_conn->conn, "query\n%s",
			 activate_acct_cond.insert_query);
		rc = mysql_db_query(mysql_conn, activate_acct_cond.insert_query);
		xfree(activate_acct_cond.insert_query);
		if (rc != SLURM_SUCCESS) {
			error("Couldn't activate acct");
			xfree(activate_acct_cond.ret_str);
			goto end_it;
		}

		xstrcatat(activate_acct_cond.txn_query,
			  &activate_acct_cond.txn_query_pos,
			  ";");
		rc = mysql_db_query(mysql_conn, activate_acct_cond.txn_query);
		if (rc != SLURM_SUCCESS) {
			error("Couldn't add txn");
			rc = SLURM_SUCCESS;
		}
	}

	/* Now activate the associations */
	ret_str = as_mysql_add_assocs_cond(mysql_conn, uid, activate_assoc);
	rc = errno;

	if (rc == SLURM_NO_CHANGE_IN_DATA) {
		if (activate_acct_cond.ret_str)
			rc = SLURM_SUCCESS;
	}

end_it:
	xfree(activate_acct_cond.insert_query);
	xfree(activate_acct_cond.txn_query);
	xfree(activate_acct_cond.user_name);

	if (rc != SLURM_SUCCESS) {
		reset_mysql_conn(mysql_conn);
		if (!activate_acct_cond.ret_str_err)
			xfree(activate_acct_cond.ret_str);
		else
			xfree(ret_str);
		errno = rc;
		return activate_acct_cond.ret_str ? activate_acct_cond.ret_str : ret_str;
	}

	if (ret_str) {
		xstrcatat(activate_acct_cond.ret_str,
			  &activate_acct_cond.ret_str_pos,
			  ret_str);
		xfree(ret_str);
	}

	if (!activate_acct_cond.ret_str) {
		DB_DEBUG(DB_ASSOC, mysql_conn->conn, "didn't affect anything");
		errno = SLURM_NO_CHANGE_IN_DATA;
		return NULL;
	}

	errno = SLURM_SUCCESS;
	return activate_acct_cond.ret_str;
}

extern List as_mysql_deactivate_accts(mysql_conn_t *mysql_conn, uint32_t uid,
				  slurmdb_account_cond_t *acct_cond)
{
	list_itr_t *itr = NULL;
	List ret_list = NULL;
	List coord_list = NULL;
	List cluster_list_tmp = NULL;
	int rc = SLURM_SUCCESS;
	char *object = NULL, *at = NULL;
	char *extra = NULL, *query = NULL,
		*name_char = NULL, *name_char_pos = NULL,
		*assoc_char = NULL, *assoc_char_pos = NULL;
	time_t now = time(NULL);
	char *user_name = NULL;
	MYSQL_RES *result = NULL;
	MYSQL_ROW row = NULL;
	bool jobs_running = 0;
	bool default_account = 0;

	if (!acct_cond) {
		error("we need something to deactivate");
		return NULL;
	}

	if (check_connection(mysql_conn) != SLURM_SUCCESS)
		return NULL;

	if (!is_user_min_admin_level(mysql_conn, uid, SLURMDB_ADMIN_OPERATOR)) {
		errno = ESLURM_ACCESS_DENIED;
		return NULL;
	}

	xstrcatat(extra, &at, "where deleted=0");

	_setup_acct_cond_limits(acct_cond, &extra, &at);

	if (!extra) {
		error("Nothing to deactivate");
		return NULL;
	}

	query = xstrdup_printf("select name from %s %s;", acct_table, extra);
	xfree(extra);
	if (!(result = mysql_db_query_ret(
		      mysql_conn, query, 0))) {
		xfree(query);
		return NULL;
	}

	ret_list = list_create(xfree_ptr);
	while ((row = mysql_fetch_row(result))) {
		char *object = xstrdup(row[0]);
		list_append(ret_list, object);

		if (name_char)
			xstrfmtcatat(name_char, &name_char_pos, ",'%s'",
				     object);
		else
			xstrfmtcatat(name_char, &name_char_pos, "name in('%s'",
				     object);
		xstrfmtcatat(assoc_char, &assoc_char_pos,
			     "%st2.lineage like '%%/%s/%%'",
			     assoc_char ? " || " : "", object);
	}

	if (name_char)
		xstrcatat(name_char, &name_char_pos, ")");

	mysql_free_result(result);

	if (!list_count(ret_list)) {
		errno = SLURM_NO_CHANGE_IN_DATA;
		DB_DEBUG(DB_ASSOC, mysql_conn->conn,
		         "didn't affect anything\n%s", query);
		xfree(query);
		return ret_list;
	}
	xfree(query);

	/* We need to deactivate these accounts from the coord's that have it */
	coord_list = as_mysql_deactivate_coord(
		mysql_conn, uid, ret_list, NULL);
	FREE_NULL_LIST(coord_list);

	user_name = uid_to_string((uid_t) uid);

	slurm_rwlock_rdlock(&as_mysql_cluster_list_lock);
	cluster_list_tmp = list_shallow_copy(as_mysql_cluster_list);
	itr = list_iterator_create(cluster_list_tmp);
	while ((object = list_next(itr))) {
#ifdef __METASTACK_OPT_SACCTMGR_ADD_USER
		uint32_t rpc_version = get_cluster_version(mysql_conn, object);
		if (rpc_version < SLURM_23_11_PROTOCOL_VERSION) {
			slurm_mutex_lock(&assoc_lock);
		}

		rc = deactivate_common(mysql_conn, DBD_DEACTIVATE_ACCOUNTS, now,
					user_name, acct_table, name_char,
					assoc_char, object, ret_list,
					&jobs_running, &default_account);

		if (rpc_version < SLURM_23_11_PROTOCOL_VERSION) {
			slurm_mutex_unlock(&assoc_lock);
		}

		if (rc != SLURM_SUCCESS)
			break;
#endif
	}
	list_iterator_destroy(itr);
	FREE_NULL_LIST(cluster_list_tmp);
	slurm_rwlock_unlock(&as_mysql_cluster_list_lock);

	xfree(user_name);
	xfree(name_char);
	xfree(assoc_char);
	if (rc == SLURM_ERROR) {
		FREE_NULL_LIST(ret_list);
		return NULL;
	}

	if (default_account)
		errno = ESLURM_NO_DEACTIVATE_DEFAULT_ACCOUNT;
	else if (jobs_running)
		errno = ESLURM_JOBS_RUNNING_ON_ASSOC;
	else
		errno = SLURM_SUCCESS;
	return ret_list;
}
#endif

extern List as_mysql_get_accts(mysql_conn_t *mysql_conn, uid_t uid,
			       slurmdb_account_cond_t *acct_cond)
{
	char *query = NULL;
	char *extra = NULL, *at = NULL;
	char *tmp = NULL;
	List acct_list = NULL;
	list_itr_t *itr = NULL;
	int set = 0;
	int i=0, is_admin=1;
	MYSQL_RES *result = NULL;
	MYSQL_ROW row;
	slurmdb_user_rec_t user;
#ifdef __METASTACK_OPT_LIST_USER
	bool list_all = false;
#endif 

	/* if this changes you will need to edit the corresponding enum */
	char *acct_req_inx[] = {
		"name",
		"description",
		"organization",
		"deleted",
		"flags",
	};
	enum {
		SLURMDB_REQ_NAME,
		SLURMDB_REQ_DESC,
		SLURMDB_REQ_ORG,
		SLURMDB_REQ_DELETED,
		SLURMDB_REQ_FLAGS,
		SLURMDB_REQ_COUNT
	};

	if (check_connection(mysql_conn) != SLURM_SUCCESS)
		return NULL;

	memset(&user, 0, sizeof(slurmdb_user_rec_t));
	user.uid = uid;

	if (slurm_conf.private_data & PRIVATE_DATA_ACCOUNTS) {
		if (!(is_admin = is_user_min_admin_level(
			      mysql_conn, uid, SLURMDB_ADMIN_OPERATOR))) {
			if (slurmdbd_conf->flags &
			    DBD_CONF_FLAG_DISABLE_COORD_DBD) {
				error("Coordinator privilege revoked with DisableCoordDBD, only admins/operators can add accounts.");
				errno = ESLURM_ACCESS_DENIED;
				return NULL;
			}
			if (!is_user_any_coord(mysql_conn, &user)) {
				error("Only admins/coordinators "
				      "can look at account usage");
				errno = ESLURM_ACCESS_DENIED;
				return NULL;
			}
		}
	}

	if (!acct_cond) {
		xstrcat(extra, "where deleted=0");
		goto empty;
	}

	if (acct_cond->flags & SLURMDB_ACCT_FLAG_DELETED)
		xstrcatat(extra, &at, "where (deleted=0 || deleted=1)");
	else
		xstrcatat(extra, &at, "where deleted=0");


	_setup_acct_cond_limits(acct_cond, &extra, &at);

#ifdef __METASTACK_OPT_LIST_USER
	if (acct_cond && acct_cond->assoc_cond &&
		!list_count(acct_cond->assoc_cond->acct_list)) {

		list_all = true;
	}
#endif

empty:

	xfree(tmp);
	xstrfmtcat(tmp, "%s", acct_req_inx[i]);
	for(i=1; i<SLURMDB_REQ_COUNT; i++) {
		xstrfmtcat(tmp, ", %s", acct_req_inx[i]);
	}

	/* This is here to make sure we are looking at only this user
	 * if this flag is set.  We also include any accounts they may be
	 * coordinator of.
	 */
	if (!is_admin && (slurm_conf.private_data & PRIVATE_DATA_ACCOUNTS)) {
		slurmdb_coord_rec_t *coord = NULL;
		set = 0;
		itr = list_iterator_create(user.coord_accts);
		while ((coord = list_next(itr))) {
			if (set) {
				xstrfmtcat(extra, " || name='%s'",
					   coord->name);
			} else {
				set = 1;
				xstrfmtcat(extra, " && (name='%s'",
					   coord->name);
			}
		}
		list_iterator_destroy(itr);
		if (set)
			xstrcat(extra,")");
	}

	query = xstrdup_printf("select %s from %s %s", tmp, acct_table, extra);
	xfree(tmp);
	xfree(extra);

	DB_DEBUG(DB_ASSOC, mysql_conn->conn, "query\n%s", query);
	if (!(result = mysql_db_query_ret(
		      mysql_conn, query, 0))) {
		xfree(query);
		return NULL;
	}
	xfree(query);

	acct_list = list_create(slurmdb_destroy_account_rec);

	if (acct_cond && acct_cond->assoc_cond &&
	    (acct_cond->flags & SLURMDB_ACCT_FLAG_WASSOC)) {
		/* We are going to be freeing the inners of
		   this list in the acct->name so we don't
		   free it here
		*/
		FREE_NULL_LIST(acct_cond->assoc_cond->acct_list);
		acct_cond->assoc_cond->acct_list = list_create(NULL);
		if (acct_cond->flags & SLURMDB_ACCT_FLAG_DELETED)
			acct_cond->assoc_cond->with_deleted = 1;
#ifdef __METASTACK_OPT_USER_DEACTIVATE
		else if (acct_cond->flags & SLURMDB_ACCT_FLAG_DEACTIVATED)
			acct_cond->assoc_cond->only_deactivated = 1;
#endif
	}

	while ((row = mysql_fetch_row(result))) {
		slurmdb_account_rec_t *acct =
			xmalloc(sizeof(slurmdb_account_rec_t));
		list_append(acct_list, acct);

		acct->name =  xstrdup(row[SLURMDB_REQ_NAME]);
		acct->description = xstrdup(row[SLURMDB_REQ_DESC]);
		acct->organization = xstrdup(row[SLURMDB_REQ_ORG]);
		acct->flags = slurm_atoul(row[SLURMDB_REQ_FLAGS]);

		if (slurm_atoul(row[SLURMDB_REQ_DELETED]))
			acct->flags |= SLURMDB_ACCT_FLAG_DELETED;

		if (acct_cond && (acct_cond->flags & SLURMDB_ACCT_FLAG_WCOORD))
			acct->coordinators =
				assoc_mgr_acct_coords(mysql_conn, acct->name);

		if (acct_cond && (acct_cond->flags &
				  SLURMDB_ACCT_FLAG_WASSOC)) {
			if (!acct_cond->assoc_cond) {
				acct_cond->assoc_cond = xmalloc(
					sizeof(slurmdb_assoc_cond_t));
			}

			list_append(acct_cond->assoc_cond->acct_list,
				    acct->name);
		}
	}
	mysql_free_result(result);

	if (acct_cond &&
	    (acct_cond->flags & SLURMDB_ACCT_FLAG_WASSOC) &&
	    acct_cond->assoc_cond
	    && list_count(acct_cond->assoc_cond->acct_list)) {
		list_itr_t *assoc_itr = NULL;
		slurmdb_account_rec_t *acct = NULL;
		slurmdb_assoc_rec_t *assoc = NULL;
#ifdef __METASTACK_OPT_LIST_USER
		List assoc_list = as_mysql_get_assocs(
			mysql_conn, uid, acct_cond->assoc_cond, list_all);
#endif

		if (!assoc_list) {
			error("no associations");
			return acct_list;
		}

#ifdef __METASTACK_ASSOC_HASH
        assoc_hash_t *assoc_hash = NULL;

        /** build hash table */
        assoc_itr = list_iterator_create(assoc_list);
        while ((assoc = list_next(assoc_itr))) {
            if (!assoc->acct)
                continue;
            
            insert_assoc_hash(&assoc_hash, assoc, assoc->acct, slurmdb_destroy_assoc_rec);
            list_remove(assoc_itr);
        }
        list_iterator_destroy(assoc_itr);

        itr = list_iterator_create(acct_list);
		while ((acct = list_next(itr))) {
            assoc_hash_t *tmp_entry = NULL;
            tmp_entry = find_assoc_entry(&assoc_hash, acct->name);

            if (tmp_entry) {
                acct->assoc_list = tmp_entry->value_assoc_list;
                tmp_entry->value_assoc_list = NULL;
            }
		}
        list_iterator_destroy(itr);
        
        destroy_assoc_hash(&assoc_hash);
#else
		itr = list_iterator_create(acct_list);
		assoc_itr = list_iterator_create(assoc_list);
		while ((acct = list_next(itr))) {
			while ((assoc = list_next(assoc_itr))) {
				if (xstrcmp(assoc->acct, acct->name))
					continue;

				if (!acct->assoc_list)
					acct->assoc_list = list_create(
						slurmdb_destroy_assoc_rec);
				list_append(acct->assoc_list, assoc);
				list_remove(assoc_itr);
			}
			list_iterator_reset(assoc_itr);
		}
		list_iterator_destroy(itr);
		list_iterator_destroy(assoc_itr);
#endif
		FREE_NULL_LIST(assoc_list);
	}

	return acct_list;
}
