'''
CauchyEC - A python wrapper for Cauchy Reed-Solomon codes from Jerasure Library.
Copright (C) 2012 Lluis Pamies-Juarez <lluis@pamies.cat>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
'''

cdef extern from "cauchy.h":
    extern int *cauchy_original_coding_matrix(int k, int m, int w)
    extern int *cauchy_xy_coding_matrix(int k, int m, int w, int *x, int *y)
    extern void cauchy_improve_coding_matrix(int k, int m, int w, int *matrix)
    extern int *cauchy_good_general_coding_matrix(int k, int m, int w)
    extern int cauchy_n_ones(int n, int w)

cdef extern from "galois.h":
    extern int galois_single_multiply(int a, int b, int w)
    extern int galois_single_divide(int a, int b, int w)
    extern int galois_log(int value, int w)
    extern int galois_ilog(int value, int w)

    extern int galois_create_log_tables(int w)
    extern int galois_logtable_multiply(int x, int y, int w)
    extern int galois_logtable_divide(int x, int y, int w)

    extern int galois_create_mult_tables(int w)
    extern int galois_multtable_multiply(int x, int y, int w)
    extern int galois_multtable_divide(int x, int y, int w)

    extern int galois_shift_multiply(int x, int y, int w)
    extern int galois_shift_divide(int x, int y, int w)

    extern int galois_create_split_w8_tables()
    extern int galois_split_w8_multiply(int x, int y)

    extern int galois_inverse(int x, int w)
    extern int galois_shift_inverse(int y, int w)

    extern int *galois_get_mult_table(int w)
    extern int *galois_get_div_table(int w)
    extern int *galois_get_log_table(int w)
    extern int *galois_get_ilog_table(int w)

    void galois_region_xor(char *r1, char *r2, char *r3, int nbytes)
    void galois_w08_region_multiply(char *region, int multby, int nbytes, char *r2, int add)
    void galois_w16_region_multiply(char *region, int multby, int nbytes, char *r2, int add)
    void galois_w32_region_multiply(char *region, int multby, int nbytes, char *r2, int add)

cdef extern from "jerasure.h":
    int *jerasure_matrix_to_bitmatrix(int k, int m, int w, int *matrix)
    int **jerasure_dumb_bitmatrix_to_schedule(int k, int m, int w, int *bitmatrix)
    int **jerasure_smart_bitmatrix_to_schedule(int k, int m, int w, int *bitmatrix)
    int ***jerasure_generate_schedule_cache(int k, int m, int w, int *bitmatrix, int smart)
    void jerasure_free_schedule(int **schedule)
    void jerasure_free_schedule_cache(int k, int m, int ***cache)
    void jerasure_do_parity(int k, char **data_ptrs, char *parity_ptr, int size)
    void jerasure_matrix_encode(int k, int m, int w, int *matrix,
                              char **data_ptrs, char **coding_ptrs, int size)
    void jerasure_bitmatrix_encode(int k, int m, int w, int *bitmatrix,
                                char **data_ptrs, char **coding_ptrs, int size, int packetsize)
    void jerasure_schedule_encode(int k, int m, int w, int **schedule,
                                      char **data_ptrs, char **coding_ptrs, int size, int packetsize)
    int jerasure_matrix_decode(int k, int m, int w, 
                              int *matrix, int row_k_ones, int *erasures,
                              char **data_ptrs, char **coding_ptrs, int size)
    int jerasure_bitmatrix_decode(int k, int m, int w, 
                                int *bitmatrix, int row_k_ones, int *erasures,
                                char **data_ptrs, char **coding_ptrs, int size, int packetsize)
    int jerasure_schedule_decode_lazy(int k, int m, int w, int *bitmatrix, int *erasures,
                                char **data_ptrs, char **coding_ptrs, int size, int packetsize,
                                int smart)
    int jerasure_schedule_decode_cache(int k, int m, int w, int ***scache, int *erasures,
                                char **data_ptrs, char **coding_ptrs, int size, int packetsize)
    int jerasure_make_decoding_matrix(int k, int m, int w, int *matrix, int *erased, 
                                      int *decoding_matrix, int *dm_ids)
    int jerasure_make_decoding_bitmatrix(int k, int m, int w, int *matrix, int *erased, 
                                      int *decoding_matrix, int *dm_ids)
    int *jerasure_erasures_to_erased(int k, int m, int *erasures)
    void jerasure_matrix_dotprod(int k, int w, int *matrix_row,
                              int *src_ids, int dest_id,
                              char **data_ptrs, char **coding_ptrs, int size)
    void jerasure_bitmatrix_dotprod(int k, int w, int *bitmatrix_row,
                                 int *src_ids, int dest_id,
                                 char **data_ptrs, char **coding_ptrs, int size, int packetsize)
    void jerasure_do_scheduled_operations(char **ptrs, int **schedule, int packetsize)
    int jerasure_invert_matrix(int *mat, int *inv, int rows, int w)
    int jerasure_invert_bitmatrix(int *mat, int *inv, int rows)
    int jerasure_invertible_matrix(int *mat, int rows, int w)
    int jerasure_invertible_bitmatrix(int *mat, int rows)
    void jerasure_print_matrix(int *matrix, int rows, int cols, int w)
    void jerasure_print_bitmatrix(int *matrix, int rows, int cols, int w)
    int *jerasure_matrix_multiply(int *m1, int *m2, int r1, int c1, int r2, int c2, int w)
    void jerasure_get_stats(double *fill_in)
    int **jerasure_generate_decoding_schedule(int k, int m, int w, int *bitmatrix, int *erasures, int smart)
